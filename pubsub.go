package contentpubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/rand"
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
// MaxAttributesPerSub >> maximum allowed number of attributes per predicate
// SubRefreshRateMin >> frequency in which a subscriber needs to resub in minutes
const (
	FaultToleranceFactor      = 3
	MaxAttributesPerPredicate = 5
	SubRefreshRateMin         = 15
)

// PubSub data structure
type PubSub struct {
	pb.UnimplementedScoutHubServer
	server     *grpc.Server
	serverAddr string

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	myBackups        []string
	myBackupsFilters map[string]*FilterTable
	mapBackupAddr    map[string]string

	interestingEvents   chan *pb.Event
	subsToForward       chan *ForwardSubRequest
	eventsToForwardUp   chan *ForwardEvent
	eventsToForwardDown chan *ForwardEvent
	terminate           chan string

	tablesLock *sync.RWMutex

	managedGroups  []*MulticastGroup
	subbedGroups   []*SubGroupView
	region         string
	subRegion      string
	premiumEvents  chan *pb.PremiumEvent
	advertiseBoard []*pb.MulticastGroupID
	advToForward   chan *ForwardAdvert

	record   *HistoryRecord
	session  int
	eventSeq int
}

// NewPubSub initializes the PubSub's data structure
// setup the server and starts processloop
func NewPubSub(dht *kaddht.IpfsDHT, region string, subRegion string) *PubSub {

	filterTable := NewFilterTable(dht)
	auxFilterTable := NewFilterTable(dht)
	mySubs := NewRouteStats()

	ps := &PubSub{
		currentFilterTable:  filterTable,
		nextFilterTable:     auxFilterTable,
		myFilters:           mySubs,
		myBackupsFilters:    make(map[string]*FilterTable),
		mapBackupAddr:       make(map[string]string),
		interestingEvents:   make(chan *pb.Event),
		premiumEvents:       make(chan *pb.PremiumEvent),
		subsToForward:       make(chan *ForwardSubRequest, 2*len(filterTable.routes)),
		eventsToForwardUp:   make(chan *ForwardEvent, 2*len(filterTable.routes)),
		eventsToForwardDown: make(chan *ForwardEvent, 2*len(filterTable.routes)),
		terminate:           make(chan string),
		advToForward:        make(chan *ForwardAdvert),
		tablesLock:          &sync.RWMutex{},
		region:              region,
		subRegion:           subRegion,
		record:              NewHistoryRecord(),
		session:             rand.Intn(9999),
		eventSeq:            0,
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

	ps.serverAddr = dialAddr
	ps.server = grpc.NewServer()
	pb.RegisterScoutHubServer(ps.server, ps)
	go ps.server.Serve(lis)
	go ps.processLoop()
	go ps.refreshingProtocol()

	return ps
}

// +++++++++++++++++++++++++++++++ ScoutSubs ++++++++++++++++++++++++++++++++

type ForwardSubRequest struct {
	dialAddr string
	sub      *pb.Subscription
}

type ForwardEvent struct {
	originalRoute string
	dialAddr      string
	event         *pb.Event
}

// MySubscribe subscribes to certain event(s) and saves
// it in myFilters for further resubing operations
func (ps *PubSub) mySubscribe(info string) error {
	fmt.Println("MySubscribe: " + ps.serverAddr)

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
		return errors.New("no address for closest peer")
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

	// Statistical Code
	ps.record.AddOperationStat("mySubscribe")

	return nil
}

// Subscribe is a remote function called by a external peer to send subscriptions
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Println("Subscribe: " + ps.serverAddr)

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

	// Statistical Code
	ps.record.AddOperationStat("Subscribe")

	return &pb.Ack{State: true, Info: ""}, nil
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

// MyUnsubscribe deletes specific predicate out of
// mySubs list which will stop the node of sending
// a subscribing operation every refreshing cycle
func (ps *PubSub) myUnsubscribe(info string) error {

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	ps.myFilters.SimpleSubtractFilter(p)

	// Statistical Code
	ps.record.AddOperationStat("myUnsubscribe")

	return nil
}

// MyPublish function is used when we want to publish an event on the overlay.
// Data is the message we want to publish and info is the representative
// predicate of that event data. The publish operation is made towards all
// attributes rendezvous in order find the way to all subscribers
func (ps *PubSub) myPublish(data string, info string) error {
	fmt.Printf("MyPublish: %s\n", ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	var dialAddr string
	for _, attr := range p.attributes {

		eventID := &pb.EventID{
			PublisherID:   peer.Encode(ps.ipfsDHT.PeerID()),
			SessionNumber: int32(ps.session),
			SeqID:         int32(ps.eventSeq),
		}

		event := &pb.Event{
			EventID:   eventID,
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
					return errors.New("no address to send")
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
			return errors.New("no address for closest peer")
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

	// Statistical Code
	ps.record.AddOperationStat("myPublish")

	return nil
}

// Publish is a remote function called by a external peer to send an Event upstream
func (ps *PubSub) Publish(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("Publish: " + ps.serverAddr)

	p, err := NewPredicate(event.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
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

	// Statistical Code
	ps.record.AddOperationStat("Publish")

	return &pb.Ack{State: true, Info: ""}, nil
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

// Notify is a remote function called by a external peer to send an Event downstream
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Print("Notify: " + ps.serverAddr)

	p, err := NewPredicate(event.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
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

	// Statistical Code
	ps.record.AddOperationStat("Notify")

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event, originalRoute string) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	if dialAddr == ps.serverAddr {
		ps.Notify(ctx, event)
	}

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
		for _, backup := range ps.currentFilterTable.routes[originalRoute].backups {
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

// updateBackup sends the new version of the filter table to the backup
func (ps *PubSub) UpdateBackup(ctx context.Context, update *pb.Update) (*pb.Ack, error) {
	fmt.Println("UpdateBackup >> " + ps.serverAddr)

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

	// Statistical Code
	ps.record.AddOperationStat("UpdateBackup")

	return &pb.Ack{State: true, Info: ""}, nil
}

// updateMyBackups basically sends updates rpcs to its backups
// to update their versions of his filter table
func (ps *PubSub) updateMyBackups(route string, info string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	for _, addrB := range ps.myBackups {
		conn, err := grpc.Dial(addrB, grpc.WithInsecure())
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
		if err != nil || !ack.State {
			ps.eraseOldFetchNewBackup(addrB)
			return errors.New("failed update")
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
		if backupAddr == nil {
			continue
		}
		aux := strings.Split(backupAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
		backups = append(backups, dialAddr)
	}

	return backups
}

// eraseOldFetchNewBackup
func (ps *PubSub) eraseOldFetchNewBackup(oldAddr string) {

	var refIndex int
	for i, backup := range ps.myBackups {
		if backup == oldAddr {
			refIndex = i
		}
	}

	candidate := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), FaultToleranceFactor+1)
	if len(candidate) != FaultToleranceFactor+1 {
		return
	}

	backupAddr := ps.ipfsDHT.FindLocal(candidate[FaultToleranceFactor]).Addrs[0]
	if backupAddr == nil {
		return
	}
	aux := strings.Split(backupAddr.String(), "/")
	newAddr := aux[2] + ":4" + aux[4][1:]
	ps.myBackups[refIndex] = newAddr

	updates, err := ps.filtersForBackupRefresh()
	if err != nil {
		return
	}

	ps.refreshOneBackup(newAddr, updates)
}

// BackupRefresh
func (ps *PubSub) BackupRefresh(stream pb.ScoutHub_BackupRefreshServer) error {
	fmt.Println("BackupRefresh >> " + ps.serverAddr)

	var i = 0
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			// Statistical Code
			ps.record.AddOperationStat("BackupRefresh")

			return stream.SendAndClose(&pb.Ack{State: true, Info: ""})
		}
		if err != nil {
			return err
		}
		if i == 0 {
			ps.myBackupsFilters[update.Sender] = nil
		}

		p, err := NewPredicate(update.Predicate)
		if err != nil {
			return err
		}

		if ps.myBackupsFilters[update.Sender] == nil {
			ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
		}

		if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
			ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats()
		}

		ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p)
		ps.mapBackupAddr[update.Route] = update.RouteAddr
		i = 1
	}
}

// refreashBackups
func (ps *PubSub) refreshAllBackups() error {

	updates, err := ps.filtersForBackupRefresh()
	if err != nil {
		return err
	}

	for _, backup := range ps.myBackups {
		err := ps.refreshOneBackup(backup, updates)
		if err != nil {
			return err
		}
	}

	return nil
}

// filtersForBackupRefresh
func (ps *PubSub) filtersForBackupRefresh() ([]*pb.Update, error) {

	var updates []*pb.Update
	for route, routeS := range ps.currentFilterTable.routes {
		routeID, err := peer.Decode(route)
		if err != nil {
			return nil, err
		}

		var routeAddr string
		addr := ps.ipfsDHT.FindLocal(routeID).Addrs[0]
		if addr != nil {
			aux := strings.Split(addr.String(), "/")
			routeAddr = aux[2] + ":4" + aux[4][1:]
		}

		for _, filters := range routeS.filters {
			for _, filter := range filters {
				u := &pb.Update{
					Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
					Route:     route,
					RouteAddr: routeAddr,
					Predicate: filter.ToString()}
				updates = append(updates, u)
			}
		}
	}

	return updates, nil
}

func (ps *PubSub) refreshOneBackup(backup string, updates []*pb.Update) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(backup, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	stream, err := client.BackupRefresh(ctx)
	if err != nil {
		return err
	}

	for _, up := range updates {
		if err := stream.Send(up); err != nil {
			return err
		}
	}

	ack, err := stream.CloseAndRecv()
	if err != nil || !ack.State {
		return err
	}

	return nil
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

// heartbeatProtocol is the routine responsible to
// refresh periodically the subscriptions of a peer
// and the filterTables after 2 subs refreshings
func (ps *PubSub) refreshingProtocol() {

	for {
		time.Sleep(SubRefreshRateMin * time.Minute)

		for _, filters := range ps.myFilters.filters {
			for _, filter := range filters {
				ps.mySubscribe(filter.ToString())
			}
		}

		time.Sleep(SubRefreshRateMin * time.Minute)

		for _, filters := range ps.myFilters.filters {
			for _, filter := range filters {
				ps.mySubscribe(filter.ToString())
			}
		}

		ps.tablesLock.Lock()
		ps.currentFilterTable = ps.nextFilterTable
		ps.nextFilterTable = NewFilterTable(ps.ipfsDHT)
		ps.refreshAllBackups()
		ps.tablesLock.Unlock()
	}
}

func (ps *PubSub) terminateService() {
	ps.terminate <- "end"
	ps.server.Stop()
	ps.ipfsDHT.Close()
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
			// Statistical-Code
			ps.record.SaveReceivedEvent(pid.Event, pid.EventID.PublisherID, "ScoutSubs")
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.premiumEvents:
			// Statistical-Code
			ps.record.SaveReceivedEvent(pid.Event, pid.GroupID.Predicate, "FastDelivery")
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.advToForward:
			ps.forwardAdvertising(pid.dialAddr, pid.adv)
		case <-ps.terminate:
			return
		}
	}
}

// ++++++++++++++++++++++++ Fast-Delivery ++++++++++++++++++++++++ //

type ForwardAdvert struct {
	dialAddr string
	adv      *pb.AdvertRequest
}

// CreateMulticastGroup
func (ps *PubSub) CreateMulticastGroup(pred string) error {

	p, err := NewPredicate(pred)
	if err != nil {
		return err
	}

	ps.managedGroups = append(ps.managedGroups, NewMulticastGroup(p, ps.serverAddr))
	ps.myAdvertiseGroup(p)

	return nil
}

func (ps *PubSub) myAdvertiseGroup(pred *Predicate) error {
	fmt.Printf("myAdvertiseGroup: %s\n", ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	var dialAddr string
	for _, attr := range pred.attributes {

		groupID := &pb.MulticastGroupID{
			OwnerAddr: ps.serverAddr,
			Predicate: pred.ToString(),
		}

		advReq := &pb.AdvertRequest{
			GroupID: groupID,
			RvId:    attr.name,
		}

		res, _ := ps.rendezvousSelfCheck(attr.name)
		if res {
			ps.advertiseBoard = append(ps.advertiseBoard, groupID)
		}

		attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(attr.name))
		attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]
		if attrAddr == nil {
			return errors.New("no address for closest peer")
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

		ack, err := client.AdvertiseGroup(ctx, advReq)
		if err != nil || !ack.State {
			alternatives := ps.alternativesToRv(attr.name)
			for _, addr := range alternatives {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("fail to dial: %v", err)
				}
				defer conn.Close()

				client := pb.NewScoutHubClient(conn)
				ack, err := client.AdvertiseGroup(ctx, advReq)
				if ack.State && err == nil {
					break
				}
			}
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("myPublish")

	return nil
}

// AdvertiseGroup
func (ps *PubSub) AdvertiseGroup(ctx context.Context, adv *pb.AdvertRequest) (*pb.Ack, error) {
	fmt.Printf("AdvertiseGroup: %s\n", ps.serverAddr)

	res, _ := ps.rendezvousSelfCheck(adv.RvId)
	if res {
		ps.advertiseBoard = append(ps.advertiseBoard, adv.GroupID)
		return &pb.Ack{State: true, Info: ""}, nil
	}

	attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(adv.RvId))
	attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]

	var dialAddr string
	if attrAddr == nil {
		return nil, errors.New("no address for closest peer")
	} else {
		aux := strings.Split(attrAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
	}

	ps.advToForward <- &ForwardAdvert{
		dialAddr: dialAddr,
		adv:      adv,
	}

	// Statistical Code
	ps.record.AddOperationStat("AdvertiseGroup")

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardAdvertising
func (ps *PubSub) forwardAdvertising(dialAddr string, adv *pb.AdvertRequest) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.AdvertiseGroup(ctx, adv)

	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(adv.RvId)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.AdvertiseGroup(ctx, adv)
			if ack.State && err == nil {
				break
			}
		}
	}
}

// myGroupSearchRequest
func (ps *PubSub) myGroupSearchRequest(pred string) error {
	fmt.Println("myGroupSearchRequest: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(pred)
	if err != nil {
		return err
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

	res, _ := ps.rendezvousSelfCheck(minAttr)
	if res {
		for _, g := range ps.returnGroupsOfInterest(p) {
			fmt.Println("Pub: " + g.OwnerAddr + " Theme: " + g.Predicate)
		}
		return nil
	}

	var dialAddr string
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	if closestAddr == nil {
		return errors.New("no address for closest peer")
	} else {
		aux := strings.Split(closestAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
	}

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	req := &pb.SearchRequest{
		Predicate: pred,
		RvID:      minAttr,
	}

	client := pb.NewScoutHubClient(conn)
	reply, err := client.GroupSearchRequest(ctx, req)
	if err != nil {
		alternatives := ps.alternativesToRv(req.RvID)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			reply, err := client.GroupSearchRequest(ctx, req)
			if err == nil {
				for _, g := range reply.Groups {
					fmt.Println("Pub: " + g.OwnerAddr + " Theme: " + g.Predicate)
				}
				break
			}
		}
	} else {
		for _, g := range reply.Groups {
			fmt.Println("Pub: " + g.OwnerAddr + " Theme: " + g.Predicate)
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("myGroupSearchRequest")

	return nil
}

// GroupSearchRequest
func (ps *PubSub) GroupSearchRequest(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {

	p, err := NewPredicate(req.Predicate)
	if err != nil {
		return nil, err
	}

	marshalSelf, err := ps.ipfsDHT.Host().ID().MarshalBinary()
	if err != nil {
		return nil, err
	}

	selfKey := key.XORKeySpace.Key(marshalSelf)
	var minAttr string
	var minID peer.ID
	var minDist *big.Int = nil

	for _, attr := range p.attributes {
		candidateID := peer.ID(kb.ConvertKey(attr.name))
		aux, err := candidateID.MarshalBinary()
		if err != nil {
			return nil, err
		}

		candidateDist := key.XORKeySpace.Distance(selfKey, key.XORKeySpace.Key(aux))
		if minDist == nil || candidateDist.Cmp(minDist) == -1 {
			minAttr = attr.name
			minID = candidateID
			minDist = candidateDist
		}
	}

	res, _ := ps.rendezvousSelfCheck(minAttr)
	if res {
		var groups map[int32]*pb.MulticastGroupID = make(map[int32]*pb.MulticastGroupID)
		for i, g := range ps.returnGroupsOfInterest(p) {
			groups[int32(i)] = g
		}

		return &pb.SearchReply{Groups: groups}, nil
	}

	var dialAddr string
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	if closestAddr == nil {
		return nil, errors.New("no address for closest peer")
	} else {
		aux := strings.Split(closestAddr.String(), "/")
		dialAddr = aux[2] + ":4" + aux[4][1:]
	}

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	reply, err := client.GroupSearchRequest(ctx, req)
	if err != nil {
		alternatives := ps.alternativesToRv(req.RvID)
		for _, addr := range alternatives {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			reply, err := client.GroupSearchRequest(ctx, req)
			if err == nil {
				// Statistical Code
				ps.record.AddOperationStat("myGroupSearchRequest")

				return reply, nil
			}
		}
	} else {
		// Statistical Code
		ps.record.AddOperationStat("myGroupSearchRequest")

		return reply, nil
	}

	return nil, errors.New("dead end search")
}

func (ps *PubSub) returnGroupsOfInterest(p *Predicate) []*pb.MulticastGroupID {

	var interestGs []*pb.MulticastGroupID
	for _, g := range ps.advertiseBoard {
		pG, _ := NewPredicate(g.Predicate)
		if pG.SimplePredicateMatch(p) {
			interestGs = append(interestGs, g)
		}
	}

	return interestGs
}

// myPremiumSubscribe
func (ps *PubSub) myPremiumSubscribe(info string, pubAddr string, pubPredicate string, cap int) error {
	fmt.Printf("myPremiumSubscribe: %s\n", ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pubP, err := NewPredicate(pubPredicate)
	if err != nil {
		return err
	}

	conn, err := grpc.Dial(pubAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	sub := &pb.PremiumSubscription{
		OwnPredicate: info,
		PubPredicate: pubPredicate,
		Addr:         ps.serverAddr,
		Region:       ps.region,
		SubRegion:    ps.subRegion,
		Cap:          int32(cap),
	}

	client := pb.NewScoutHubClient(conn)
	ack, err := client.PremiumSubscribe(ctx, sub)
	if ack.State && err == nil {
		subG := &SubGroupView{
			pubAddr:   pubAddr,
			predicate: pubP,
			helping:   false,
			attrTrees: make(map[string]*RangeAttributeTree),
		}

		ps.subbedGroups = append(ps.subbedGroups, subG)

		// Statistical Code
		ps.record.AddOperationStat("myPremiumSubscribe")

		return nil
	} else {
		return errors.New("failed my premium subscribe")
	}
}

// PremiumSubscribe
func (ps *PubSub) PremiumSubscribe(ctx context.Context, sub *pb.PremiumSubscription) (*pb.Ack, error) {
	fmt.Printf("PremiumSubscribe: %s\n", ps.serverAddr)

	pubP, err1 := NewPredicate(sub.PubPredicate)
	if err1 != nil {
		return &pb.Ack{State: false, Info: ""}, err1
	}

	subP, err2 := NewPredicate(sub.OwnPredicate)
	if err2 != nil {
		return &pb.Ack{State: false, Info: ""}, err2
	}

	for _, mg := range ps.managedGroups {
		if mg.predicate.Equal(pubP) {
			mg.addSubToGroup(sub.Addr, int(sub.Cap), sub.Region, sub.SubRegion, subP)
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("PremiumSubscribe")

	return &pb.Ack{State: true, Info: ""}, nil
}

// myPremiumUnsubscribe
func (ps *PubSub) myPremiumUnsubscribe(pubPred string, pubAddr string) error {
	fmt.Printf("MyPremiumUnsubscribe: %s\n", ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pubP, err := NewPredicate(pubPred)
	if err != nil {
		return err
	}

	for i, sG := range ps.subbedGroups {
		if sG.predicate.Equal(pubP) && sG.pubAddr == pubAddr {
			conn, err := grpc.Dial(pubAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			protoSub := &pb.PremiumSubscription{
				Region:       ps.region,
				SubRegion:    ps.subRegion,
				Addr:         ps.serverAddr,
				PubPredicate: pubPred,
			}

			client := pb.NewScoutHubClient(conn)
			ack, err := client.PremiumUnsubscribe(ctx, protoSub)
			if !ack.State && err != nil {
				return errors.New("failed unsubscribing")
			}

			if i == 0 {
				ps.subbedGroups = ps.subbedGroups[1:]
			} else if len(ps.subbedGroups) == i+1 {
				ps.subbedGroups = ps.subbedGroups[:i-1]
			} else {
				ps.subbedGroups = append(ps.subbedGroups[:i-1], ps.subbedGroups[i+1:]...)
			}
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("myPremiumUnsubscribe")

	return nil
}

// PremiumUnsubscribe
func (ps *PubSub) PremiumUnsubscribe(ctx context.Context, sub *pb.PremiumSubscription) (*pb.Ack, error) {
	fmt.Printf("PremiumUnsubscribe: %s\n", ps.serverAddr)

	pubP, err1 := NewPredicate(sub.PubPredicate)
	if err1 != nil {
		return &pb.Ack{State: false, Info: ""}, err1
	}

	for _, mg := range ps.managedGroups {
		if mg.predicate.Equal(pubP) {
			mg.RemoveSubFromGroup(sub)
			return &pb.Ack{State: true, Info: ""}, nil
		}
	}

	for _, sg := range ps.subbedGroups {
		if sg.predicate.Equal(pubP) {
			sg.RemoveSub(sub)
			return &pb.Ack{State: true, Info: ""}, nil
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("PremiumSubscribe")

	return &pb.Ack{State: true, Info: ""}, nil
}

// myPremiumPublish
func (ps *PubSub) myPremiumPublish(grpPred string, event string, eventInfo string) error {
	fmt.Printf("MyPremiumPublish: %s\n", ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	gP, err1 := NewPredicate(grpPred)
	if err1 != nil {
		return err1
	}

	eP, err2 := NewPredicate(eventInfo)
	if err2 != nil {
		return err2
	}

	var mGrp *MulticastGroup
	for _, grp := range ps.managedGroups {
		if grp.predicate.Equal(gP) {
			mGrp = grp
			break
		}
	}

	if mGrp == nil {
		return nil
	}

	gID := &pb.MulticastGroupID{
		OwnerAddr: ps.serverAddr,
		Predicate: grpPred,
	}
	premiumE := &pb.PremiumEvent{
		GroupID:   gID,
		Event:     event,
		EventPred: eventInfo,
	}

	var helperFailedSubs []*SubData = nil
	for _, tracker := range mGrp.trackHelp {
		conn, err := grpc.Dial(tracker.helper.addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)
		ack, err := client.PremiumPublish(ctx, premiumE)
		if err != nil || !ack.State {
			helperFailedSubs = append(helperFailedSubs, mGrp.trackHelp[tracker.helper.addr].subsDelegated...)
			mGrp.StopDelegating(tracker, false)
		}
	}

	before := len(mGrp.helpers)
	for _, sub := range helperFailedSubs {
		mGrp.addSubToGroup(sub.addr, sub.capacity, sub.region, sub.subRegion, sub.pred)
	}
	aux := len(mGrp.helpers) - before

	for _, sub := range append(mGrp.helpers[len(mGrp.helpers)-aux:len(mGrp.helpers)], mGrp.AddrsToPublishEvent(eP)...) {
		conn, err := grpc.Dial(sub.addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)
		client.PremiumPublish(ctx, premiumE)
	}

	// Statistical Code
	ps.record.AddOperationStat("myPremiumPublish")

	return nil
}

// PremiumPublish
func (ps *PubSub) PremiumPublish(ctx context.Context, event *pb.PremiumEvent) (*pb.Ack, error) {
	fmt.Printf("PremiumPublish: %s\n", ps.serverAddr)

	pubP, err := NewPredicate(event.GroupID.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: ""}, err
	}

	for _, sg := range ps.subbedGroups {
		if sg.predicate.Equal(pubP) {
			if sg.helping {
				eP, err := NewPredicate(event.EventPred)
				if err != nil {
					return &pb.Ack{State: false, Info: ""}, err
				}

				for _, sub := range sg.AddrsToPublishEvent(eP) {
					conn, err := grpc.Dial(sub.addr, grpc.WithInsecure())
					if err != nil {
						log.Fatalf("fail to dial: %v", err)
					}
					defer conn.Close()

					client := pb.NewScoutHubClient(conn)
					client.PremiumPublish(ctx, event)
				}
			}

			ps.premiumEvents <- event
			break
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("PremiumPublish")

	return &pb.Ack{State: true, Info: ""}, nil
}

// RequestHelp
func (ps *PubSub) RequestHelp(ctx context.Context, req *pb.HelpRequest) (*pb.Ack, error) {
	fmt.Printf("RequestHelp: %s\n", ps.serverAddr)

	p, err := NewPredicate(req.GroupID.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: ""}, err
	}

	for _, grp := range ps.subbedGroups {
		if grp.pubAddr == req.GroupID.OwnerAddr && grp.predicate.Equal(p) && !grp.helping {
			err := grp.SetHasHelper(req)
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}

			break
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("RequestHelp")

	return &pb.Ack{State: true, Info: ""}, nil
}

// DelegateSubToHelper
func (ps *PubSub) DelegateSubToHelper(ctx context.Context, sub *pb.DelegateSub) (*pb.Ack, error) {
	fmt.Printf("DelegateSubToHelper: %s\n", ps.serverAddr)

	p, err := NewPredicate(sub.GroupID.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: ""}, err
	}

	for _, grp := range ps.subbedGroups {
		if grp.pubAddr == sub.GroupID.OwnerAddr && grp.predicate.Equal(p) && grp.helping {
			err := grp.AddSub(sub.Sub)
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}

			break
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("DelegateSubToHelper")

	return &pb.Ack{State: true, Info: ""}, nil
}
