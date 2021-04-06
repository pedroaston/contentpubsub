package contentpubsub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	key "github.com/libp2p/go-libp2p-kbucket/keyspace"
	pb "github.com/pedroaston/contentpubsub/pb"

	"google.golang.org/grpc"
)

// FaultToleranceFactor >> number of backups
// MaxAttributesPerSub >> maximum allowed number of attributes per predicate (TODO)
// SubRefreshRateMin >> frequency in which a subscriber needs to resub in minutes
const (
	FaultToleranceFactor      = 3
	MaxAttributesPerPredicate = 5
	SubRefreshRateMin         = 15
)

// PubSub data structure
type PubSub struct {
	pb.UnimplementedScoutHubServer
	server *grpc.Server

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	myBackups        []string
	myBackupsFilters map[string]*FilterTable
	mapBackupAddr    map[string]string

	interestingEvents   chan string
	subsToForward       chan *ForwardSubRequest
	eventsToForwardUp   chan *ForwardEvent
	eventsToForwardDown chan *ForwardEvent
	terminate           chan string

	tablesLock *sync.RWMutex

	managedGroups []*MulticastGroup
}

// NewPubSub initializes the PubSub's data structure
// setup the server and starts processloop
func NewPubSub(dht *kaddht.IpfsDHT) *PubSub {

	filterTable := NewFilterTable(dht)
	auxFilterTable := NewFilterTable(dht)
	mySubs := NewRouteStats()

	ps := &PubSub{
		currentFilterTable:  filterTable,
		nextFilterTable:     auxFilterTable,
		myFilters:           mySubs,
		myBackupsFilters:    make(map[string]*FilterTable),
		mapBackupAddr:       make(map[string]string),
		interestingEvents:   make(chan string),
		subsToForward:       make(chan *ForwardSubRequest, 2*len(filterTable.routes)),
		eventsToForwardUp:   make(chan *ForwardEvent, 2*len(filterTable.routes)),
		eventsToForwardDown: make(chan *ForwardEvent, 2*len(filterTable.routes)),
		terminate:           make(chan string),
		tablesLock:          &sync.RWMutex{},
	}

	ps.ipfsDHT = dht
	ps.myBackups = ps.getBackups()

	addr := ps.ipfsDHT.Host().Addrs()[0]
	aux := strings.Split(addr.String(), "/")
	dialAddr := aux[2] + ":4" + aux[4][1:]

	lis, err := net.Listen("tcp", dialAddr)
	if err != nil {
		return nil
	}

	ps.server = grpc.NewServer()
	pb.RegisterScoutHubServer(ps.server, ps)
	go ps.server.Serve(lis)
	go ps.processLoop()
	go ps.refreshingProtocol()

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Print("Subscribe: ")
	fmt.Println(ps.ipfsDHT.PeerID())

	p, err := NewPredicate(sub.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	var aux []string
	for _, addr := range sub.Backups {
		aux = append(aux, addr)
	}
	ps.currentFilterTable.routes[sub.PeerID].routeLock.Lock()
	ps.currentFilterTable.routes[sub.PeerID].backups = aux
	ps.currentFilterTable.routes[sub.PeerID].routeLock.Unlock()

	ps.tablesLock.RLock()
	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	alreadyDone, pNew := ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	ps.tablesLock.RUnlock()

	if alreadyDone {
		return &pb.Ack{State: true, Info: ""}, nil
	} else if pNew != nil {
		sub.Predicate = pNew.ToString()
	}

	isRv, nextHop := ps.rendezvousSelfCheck(sub.RvId)
	if !isRv && nextHop != "" {
		var dialAddr string
		closestAddr := ps.ipfsDHT.FindLocal(nextHop).Addrs[0]
		if closestAddr == nil {
			return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
		} else {
			aux := strings.Split(closestAddr.String(), "/")
			dialAddr = aux[2] + ":4" + aux[4][1:]
		}

		ps.updateMyBackups(sub.PeerID, sub.Predicate)

		var backups map[int32]string = make(map[int32]string)
		for i, backup := range ps.myBackups {
			backups[int32(i)] = backup
		}

		subForward := &pb.Subscription{
			PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
			Predicate: sub.Predicate,
			RvId:      sub.RvId,
			Backups:   backups,
		}

		ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// updateBackup sends the new version of the filter table to the backup
func (ps *PubSub) UpdateBackup(ctx context.Context, update *pb.Update) (*pb.Ack, error) {

	p, err := NewPredicate(update.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	} else if _, ok := ps.myBackupsFilters[update.Sender]; !ok {
		ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
	}

	if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
		ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats()
	}

	ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p)
	ps.mapBackupAddr[update.Route] = update.RouteAddr

	return &pb.Ack{State: true, Info: ""}, nil
}

// Publish is a remote function called by a external peer to send an Event upstream
func (ps *PubSub) Publish(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Print("Publish: ")
	fmt.Println(ps.ipfsDHT.PeerID())

	p, err := NewPredicate(event.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event.Event
	}

	isRv, nextHop := ps.rendezvousSelfCheck(event.RvId)
	if !isRv && nextHop != "" {
		var dialAddr string
		nextAddr := ps.ipfsDHT.FindLocal(nextHop).Addrs[0]
		if nextAddr == nil {
			return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
		} else {
			aux := strings.Split(nextAddr.String(), "/")
			dialAddr = aux[2] + ":4" + aux[4][1:]
		}

		ps.eventsToForwardUp <- &ForwardEvent{dialAddr: dialAddr, event: event}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	}

	ps.tablesLock.RLock()
	for next, route := range ps.currentFilterTable.routes {
		if next == event.LastHop {
			continue
		}

		if route.IsInterested(p) {
			var dialAddr string
			nextID, err := peer.Decode(next)
			if err != nil {
				ps.tablesLock.RUnlock()
				return &pb.Ack{State: false, Info: "decoding failed"}, err
			}

			nextAddr := ps.ipfsDHT.FindLocal(nextID).Addrs[0]
			if nextAddr == nil {
				ps.tablesLock.RUnlock()
				return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
			} else {
				aux := strings.Split(nextAddr.String(), "/")
				dialAddr = aux[2] + ":4" + aux[4][1:]
			}

			ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event, originalRoute: next}
		}
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// Notify is a remote function called by a external peer to send an Event downstream
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Print("Notify: ")
	fmt.Println(ps.ipfsDHT.PeerID())

	p, err := NewPredicate(event.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event.Event
	}

	ps.tablesLock.RLock()
	if event.Backup == "" {
		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {
				var dialAddr string
				nextID, err := peer.Decode(next)
				if err != nil {
					ps.tablesLock.RUnlock()
					return &pb.Ack{State: false, Info: "decoding failed"}, err
				}

				nextAddr := ps.ipfsDHT.FindLocal(nextID).Addrs[0]
				if nextAddr == nil {
					ps.tablesLock.RUnlock()
					return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
				} else {
					aux := strings.Split(nextAddr.String(), "/")
					dialAddr = aux[2] + ":4" + aux[4][1:]
				}

				ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event, originalRoute: next}
			}
		}
	} else {
		for next, route := range ps.myBackupsFilters[event.Backup].routes {
			if route.IsInterested(p) {
				event.Backup = ""

				nextAddr := ps.mapBackupAddr[next]
				ps.eventsToForwardDown <- &ForwardEvent{dialAddr: nextAddr, event: event}
			}
		}
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// MySubscribe subscribes to certain event(s) and saves
// it in myFilters for further resubing operations
func (ps *PubSub) MySubscribe(info string) error {
	fmt.Print("MySubscribe: ")
	fmt.Println(ps.ipfsDHT.PeerID())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	alreadyDone, pNew := ps.myFilters.SimpleAddSummarizedFilter(p)
	if alreadyDone {
		return nil
	} else if pNew != nil {
		p = pNew
	}

	marshalSelf, err := ps.ipfsDHT.Host().ID().MarshalBinary()
	if err != nil {
		return err
	}

	selfKey := key.XORKeySpace.Key(marshalSelf)
	var minAttr string
	var minID peer.ID
	var minDist *big.Int = nil

	for _, attr := range p.attributes {
		candidateID := peer.ID(kb.ConvertKey(attr.name))
		aux, err := candidateID.MarshalBinary()
		if err != nil {
			return err
		}

		candidateDist := key.XORKeySpace.Distance(selfKey, key.XORKeySpace.Key(aux))
		if minDist == nil || candidateDist.Cmp(minDist) == -1 {
			minAttr = attr.name
			minID = candidateID
			minDist = candidateDist
		}
	}

	var dialAddr string
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	if closestAddr == nil {
		return errors.New("No address for closest peer")
	} else {
		aux := strings.Split(closestAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
	}

	res, _ := ps.rendezvousSelfCheck(minAttr)
	if res {
		return nil
	}

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	sub := &pb.Subscription{
		PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
		Predicate: info,
		RvId:      minAttr,
	}

	ack, err := client.Subscribe(ctx, sub)
	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(sub.RvId)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Subscribe(ctx, sub)
			if ack.State && err == nil {
				break
			}
		}
	}

	return nil
}

// updateMyBackups basically sends updates rpcs to its backups
// to update their versions of his filter table
func (ps *PubSub) updateMyBackups(route string, info string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	for _, addr := range ps.myBackups {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		routeID, err := peer.Decode(route)
		if err != nil {
			return err
		}

		var routeAddr string
		addr := ps.ipfsDHT.FindLocal(routeID).Addrs[0]
		if addr != nil {
			aux := strings.Split(addr.String(), "/")
			routeAddr = aux[2] + ":4" + aux[4][1:]
		}

		client := pb.NewScoutHubClient(conn)
		update := &pb.Update{
			Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
			Route:     route,
			RouteAddr: routeAddr,
			Predicate: info,
		}

		ack, err := client.UpdateBackup(ctx, update)

		if !ack.State || err != nil {
			return errors.New("failed update")
		}

	}

	return nil
}

// MyUnsubscribe deletes specific predicate out of
// mySubs list which will stop the node of sending
// a subscribing operation every refreshing cycle
func (ps *PubSub) MyUnsubscribe(info string) error {

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	ps.myFilters.SimpleSubtractFilter(p)

	return nil
}

// MyPublish function is used when we want to publish an event on the overlay.
// Data is the message we want to publish and info is the representative
// predicate of that event data. The publish operation is made towards all
// attributes rendezvous in order find the way to all subscribers
func (ps *PubSub) MyPublish(data string, info string) error {
	fmt.Print("MyPublish: ")
	fmt.Println(ps.ipfsDHT.PeerID())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	var dialAddr string
	for _, attr := range p.attributes {

		event := &pb.Event{
			Event:     data,
			Predicate: info,
			RvId:      attr.name,
			LastHop:   peer.Encode(ps.ipfsDHT.PeerID()),
			Backup:    "",
		}

		ps.tablesLock.Lock()
		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {
				var dialAddr string
				nextID, err := peer.Decode(next)
				if err != nil {
					return err
				}

				nextAddr := ps.ipfsDHT.FindLocal(nextID).Addrs[0]
				if nextAddr == nil {
					return errors.New("no address to send!")
				} else {
					aux := strings.Split(nextAddr.String(), "/")
					dialAddr = aux[2] + ":4" + aux[4][1:]
				}

				ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event}
			}
		}
		ps.tablesLock.Unlock()

		res, _ := ps.rendezvousSelfCheck(attr.name)
		if res {
			continue
		}

		attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(attr.name))
		attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]
		if attrAddr == nil {
			return errors.New("No address for closest peer")
		} else {
			aux := strings.Split(attrAddr.String(), "/")
			dialAddr = aux[2] + ":4" + aux[4][1:]
		}

		conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)

		ack, err := client.Publish(ctx, event)
		if err != nil || !ack.State {
			alternatives := ps.alternativesToRv(event.RvId)
			for _, addr := range alternatives {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("fail to dial: %v", err)
				}
				defer conn.Close()

				client := pb.NewScoutHubClient(conn)
				ack, err := client.Publish(ctx, event)
				if ack.State && err == nil {
					break
				}
			}
		}
	}

	return nil
}

// getBackups selects f backups peers for the node,
// which are the ones closer its own ID
func (ps *PubSub) getBackups() []string {

	var backups []string

	var dialAddr string
	for _, backup := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), FaultToleranceFactor) {
		backupAddr := ps.ipfsDHT.FindLocal(backup).Addrs[0]
		aux := strings.Split(backupAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
		backups = append(backups, dialAddr)
	}

	return backups
}

// forwardSub is called upon finishing the processing a
// received subscription that needs forwarding
func (ps *PubSub) forwardSub(dialAddr string, sub *pb.Subscription) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Subscribe(ctx, sub)

	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(sub.RvId)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Subscribe(ctx, sub)
			if ack.State && err == nil {
				break
			}
		}
	}
}

// forwardEventUp is called upon receiving the request to keep forward a event
// towards a rendezvous by calling another publish operation towards it
func (ps *PubSub) forwardEventUp(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	event.LastHop = peer.Encode(ps.ipfsDHT.PeerID())
	ack, err := client.Publish(ctx, event)

	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(event.RvId)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Publish(ctx, event)
			if ack.State && err == nil {
				break
			}
		}
	}
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event, originalRoute string) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	event.LastHop = peer.Encode(ps.ipfsDHT.PeerID())
	ack, err := client.Notify(ctx, event)

	if err != nil || !ack.State {
		event.Backup = originalRoute
		for _, backup := range ps.currentFilterTable.routes[originalRoute].backups { // TAGME
			conn, err := grpc.Dial(backup, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Notify(ctx, event)
			if ack.State && err == nil {
				break
			}
		}
	}
}

// rendezvousSelfCheck evaluates if the peer is the rendezvous node
// and if not it returns the peerID of the next subscribing hop
func (ps *PubSub) rendezvousSelfCheck(rvID string) (bool, peer.ID) {

	selfID := ps.ipfsDHT.PeerID()
	closestID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(kb.ConvertKey(rvID)))

	if kb.Closer(selfID, closestID, rvID) {
		return true, ""
	}

	return false, closestID
}

// alternativesToRv checks for alternative ways to reach RV
func (ps *PubSub) alternativesToRv(rvID string) []string {

	var validAlt []string
	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ID(kb.ConvertKey(rvID)), FaultToleranceFactor)

	for _, ID := range closestIDs {
		if kb.Closer(selfID, ID, rvID) {
			attrAddr := ps.ipfsDHT.FindLocal(ID).Addrs[0]
			if attrAddr != nil {
				aux := strings.Split(attrAddr.String(), "/")
				addr := aux[2] + ":4" + aux[4][1:]
				validAlt = append(validAlt, addr)
			}
		}
	}

	return validAlt
}

type ForwardSubRequest struct {
	dialAddr string
	sub      *pb.Subscription
}

type ForwardEvent struct {
	originalRoute string
	dialAddr      string
	event         *pb.Event
}

// heartbeatProtocol is the routine responsible to
// refresh periodically the subscriptions of a peer
// and the filterTables after 2 subs refreshings
func (ps *PubSub) refreshingProtocol() {

	for {
		time.Sleep(SubRefreshRateMin * time.Minute)

		for _, filters := range ps.myFilters.filters {
			for _, filter := range filters {
				ps.MySubscribe(filter.ToString())
			}
		}

		time.Sleep(SubRefreshRateMin * time.Minute)

		for _, filters := range ps.myFilters.filters {
			for _, filter := range filters {
				ps.MySubscribe(filter.ToString())
			}
		}

		ps.tablesLock.Lock()
		ps.currentFilterTable = ps.nextFilterTable
		ps.nextFilterTable = NewFilterTable(ps.ipfsDHT)
		ps.tablesLock.Unlock()
	}
}

func (ps *PubSub) Terminate() {
	ps.terminate <- "end"
	ps.server.Stop()
}

// processLopp
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.subsToForward:
			ps.forwardSub(pid.dialAddr, pid.sub)
		case pid := <-ps.eventsToForwardUp:
			ps.forwardEventUp(pid.dialAddr, pid.event)
		case pid := <-ps.eventsToForwardDown:
			ps.forwardEventDown(pid.dialAddr, pid.event, pid.originalRoute)
		case pid := <-ps.interestingEvents:
			fmt.Println("Received Event at: ")
			fmt.Println(ps.ipfsDHT.PeerID())
			fmt.Println(">> " + pid)
		case <-ps.terminate:
			return
		}
	}
}

// ++++++++++++++++++++++++ Fast-Delivery ++++++++++++++++++++++++ //

// CreateMulticastGroup
// Proto Version
func (ps *PubSub) CreateMulticastGroup(info string) error {

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	ps.managedGroups = append(ps.managedGroups, NewMulticastGroup(p))

	return nil
}

// CloseMulticastGroup
// Proto Version
func (ps *PubSub) CloseMulticastGroup(info string) error {

	return nil
}

// MyPremiumSubscribe
// Proto Version
func (ps *PubSub) MyPremiumSubscribe(info string) error {

	return nil
}

// MyPremiumUnsubscribe
// Proto Version
func (ps *PubSub) MyPremiumUnsubscribe(info string) error {

	return nil
}

func (ps *PubSub) PremiumPublish(info string, event string) error {

	return nil
}
