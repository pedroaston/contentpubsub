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
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	key "github.com/libp2p/go-libp2p-kbucket/keyspace"
	pb "github.com/pedroaston/contentpubsub/pb"

	"google.golang.org/grpc"
)

type PubSub struct {
	maxSubsPerRegion          int
	powerSubsPoolSize         int
	maxAttributesPerPredicate int
	faultToleranceFactor      int
	region                    string

	pb.UnimplementedScoutHubServer
	server     *grpc.Server
	serverAddr string

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	myBackups        []string
	myBackupsFilters map[string]*FilterTable

	rvCache []string

	interestingEvents   chan *pb.Event
	subsToForward       chan *ForwardSubRequest
	eventsToForwardUp   chan *ForwardEvent
	eventsToForwardDown chan *ForwardEvent
	heartbeatTicker     *time.Ticker
	refreshTicker       *time.Ticker
	terminate           chan string

	tablesLock *sync.RWMutex
	upBackLock *sync.RWMutex

	managedGroups         []*MulticastGroup
	subbedGroups          []*SubGroupView
	premiumEvents         chan *pb.PremiumEvent
	currentAdvertiseBoard []*pb.MulticastGroupID
	nextAdvertiseBoard    []*pb.MulticastGroupID
	advToForward          chan *ForwardAdvert
	capacity              int

	record   *HistoryRecord
	session  int
	eventSeq int
}

// NewPubSub initializes the PubSub's data structure
// sets up the server and starts processloop
func NewPubSub(dht *kaddht.IpfsDHT, cfg *SetupPubSub) *PubSub {

	filterTable := NewFilterTable(dht)
	auxFilterTable := NewFilterTable(dht)
	mySubs := NewRouteStats("no need")

	ps := &PubSub{
		maxSubsPerRegion:          cfg.MaxSubsPerRegion,
		powerSubsPoolSize:         cfg.PowerSubsPoolSize,
		maxAttributesPerPredicate: cfg.MaxAttributesPerPredicate,
		faultToleranceFactor:      cfg.FaultToleranceFactor,
		region:                    cfg.Region,
		currentFilterTable:        filterTable,
		nextFilterTable:           auxFilterTable,
		myFilters:                 mySubs,
		myBackupsFilters:          make(map[string]*FilterTable),
		interestingEvents:         make(chan *pb.Event, cfg.ConcurrentProcessingFactor),
		premiumEvents:             make(chan *pb.PremiumEvent, cfg.ConcurrentProcessingFactor),
		subsToForward:             make(chan *ForwardSubRequest, cfg.ConcurrentProcessingFactor),
		eventsToForwardUp:         make(chan *ForwardEvent, cfg.ConcurrentProcessingFactor),
		eventsToForwardDown:       make(chan *ForwardEvent, cfg.ConcurrentProcessingFactor),
		terminate:                 make(chan string),
		advToForward:              make(chan *ForwardAdvert),
		capacity:                  cfg.Capacity,
		heartbeatTicker:           time.NewTicker(cfg.SubRefreshRateMin * time.Minute),
		refreshTicker:             time.NewTicker(2 * cfg.SubRefreshRateMin * time.Minute),
		tablesLock:                &sync.RWMutex{},
		upBackLock:                &sync.RWMutex{},
		record:                    NewHistoryRecord(),
		session:                   rand.Intn(9999),
		eventSeq:                  0,
	}

	ps.ipfsDHT = dht
	ps.myBackups = ps.getBackups()

	dialAddr := addrForPubSubServer(ps.ipfsDHT.Host().Addrs()[0])
	lis, err := net.Listen("tcp", dialAddr)
	if err != nil {
		return nil
	}

	ps.serverAddr = dialAddr
	ps.server = grpc.NewServer()
	pb.RegisterScoutHubServer(ps.server, ps)
	go ps.server.Serve(lis)
	go ps.processLoop()

	return ps
}

// +++++++++++++++++++++++++++++++ ScoutSubs ++++++++++++++++++++++++++++++++

type ForwardSubRequest struct {
	dialAddr string
	sub      *pb.Subscription
}

type ForwardEvent struct {
	dialAddr string
	event    *pb.Event
}

// MySubscribe subscribes to certain event(s) and saves
// it in myFilters for further resubing operations and
// assess if node is interested in the events it receives
func (ps *PubSub) MySubscribe(info string) error {
	fmt.Println("MySubscribe: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	_, pNew := ps.myFilters.SimpleAddSummarizedFilter(p)
	if pNew != nil {
		p = pNew
	}

	for _, attr := range p.attributes {
		isRv, _ := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			return nil
		}
	}

	minID, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
	}

	var dialAddr string
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	if closestAddr == nil {
		return errors.New("no address for closest peer")
	} else {
		dialAddr = addrForPubSubServer(closestAddr)
	}

	sub := &pb.Subscription{
		PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
		Predicate: info,
		RvId:      minAttr,
		IntAddr:   ps.serverAddr,
	}

	ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: sub}

	return nil
}

// closerAttrRvToSelf returns the closest ID node from all the Rv nodes
// of each of a subscription predicate attributes, so that the subscriber
// will send the subscription on the minimal number of hops
func (ps *PubSub) closerAttrRvToSelf(p *Predicate) (peer.ID, string, error) {

	marshalSelf, err := ps.ipfsDHT.Host().ID().MarshalBinary()
	if err != nil {
		return "", "", err
	}

	selfKey := key.XORKeySpace.Key(marshalSelf)
	var minAttr string
	var minID peer.ID
	var minDist *big.Int = nil

	for _, attr := range p.attributes {
		candidateID := peer.ID(kb.ConvertKey(attr.name))
		aux, err := candidateID.MarshalBinary()
		if err != nil {
			return "", "", err
		}

		candidateDist := key.XORKeySpace.Distance(selfKey, key.XORKeySpace.Key(aux))
		if minDist == nil || candidateDist.Cmp(minDist) == -1 {
			minAttr = attr.name
			minID = candidateID
			minDist = candidateDist
		}
	}

	return minID, minAttr, nil
}

// Subscribe is a remote function called by a external peer to send
// subscriptions towards the rendezvous node
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Println("Subscribe: " + ps.serverAddr)

	p, err := NewPredicate(sub.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	var aux []string
	for _, addr := range sub.Backups {
		aux = append(aux, addr)
	}

	if ps.currentFilterTable.routes[sub.PeerID] == nil {
		ps.tablesLock.Lock()
		ps.currentFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
		ps.tablesLock.Unlock()
	} else if ps.nextFilterTable.routes[sub.PeerID] == nil {
		ps.tablesLock.Lock()
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
		ps.tablesLock.Unlock()
	}

	ps.tablesLock.RLock()
	ps.currentFilterTable.routes[sub.PeerID].backups = aux
	ps.nextFilterTable.routes[sub.PeerID].backups = aux
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
		ps.tablesLock.RLock()
		dialAddr := ps.currentFilterTable.routes[peer.Encode(nextHop)].addr
		ps.tablesLock.RUnlock()

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
			IntAddr:   ps.serverAddr,
		}

		ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	} else {
		ps.updateRvRegion(sub.PeerID, sub.Predicate, sub.RvId)
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardSub is called once a received subscription
// still needs to be forward towards the rendevous
func (ps *PubSub) forwardSub(dialAddr string, sub *pb.Subscription) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			ctxB, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if addr == ps.serverAddr {
				break
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Subscribe(ctxB, sub)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// MyUnsubscribe deletes a specific predicate out of mySubs
// list which will stop the refreshing of thatsub and stop
// delivering to the user those contained events
func (ps *PubSub) MyUnsubscribe(info string) error {
	fmt.Printf("myUnsubscribe: %s\n", ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
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
	fmt.Println("myPublish: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

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
			Backup:    false,
			BirthTime: time.Now().Format(time.StampMilli),
		}

		ps.tablesLock.RLock()
		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {

				dialAddr := route.addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Backup:        event.Backup,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr: dialAddr,
					event:    newE,
				}
			}

		}
		ps.tablesLock.RUnlock()

		isRv, nextRvHop := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			continue
		}

		ps.tablesLock.RLock()
		dialAddr := ps.currentFilterTable.routes[peer.Encode(nextRvHop)].addr
		ps.tablesLock.RUnlock()

		ps.eventsToForwardUp <- &ForwardEvent{dialAddr: dialAddr, event: event}
	}

	ps.eventSeq++
	return nil
}

// Publish is a remote function called by a external peer to send an Event upstream
func (ps *PubSub) Publish(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("Publish >> " + ps.serverAddr)

	p, err := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
	}

	isRv, nextHop := ps.rendezvousSelfCheck(event.RvId)
	if !isRv && nextHop != "" {

		ps.tablesLock.RLock()
		dialAddr := ps.currentFilterTable.routes[peer.Encode(nextHop)].addr
		ps.tablesLock.RUnlock()

		ps.eventsToForwardUp <- &ForwardEvent{dialAddr: dialAddr, event: event}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	}

	eIDRv := fmt.Sprintf("%s%d%d", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID)
	for _, cached := range ps.rvCache {
		if eIDRv == cached {
			ps.record.AddOperationStat("Publish")
			return &pb.Ack{State: true, Info: ""}, nil
		}
	}

	ps.rvCache = append(ps.rvCache, eIDRv)

	ps.tablesLock.RLock()
	for next, route := range ps.currentFilterTable.routes {
		if next == event.LastHop {
			continue
		}

		if route.IsInterested(p) {

			dialAddr := route.addr
			newE := &pb.Event{
				Event:         event.Event,
				OriginalRoute: next,
				Backup:        event.Backup,
				Predicate:     event.Predicate,
				RvId:          event.RvId,
				EventID:       event.EventID,
				BirthTime:     event.BirthTime,
				LastHop:       event.LastHop,
			}
			ps.eventsToForwardDown <- &ForwardEvent{
				dialAddr: dialAddr,
				event:    newE,
			}
		}
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardEventUp is called upon receiving the request to keep forward a event
// towards a rendezvous by calling another publish operation towards it
func (ps *PubSub) forwardEventUp(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			ctxB, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if addr == ps.serverAddr {
				event.Backup = true
				ps.Notify(ctxB, event)
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Publish(ctxB, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// Notify is a remote function called by a external peer to send an Event downstream
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Print("Notify: " + ps.serverAddr)

	p, err := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	for _, attr := range p.attributes {
		isRv, _ := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			return &pb.Ack{State: true, Info: ""}, nil
		}
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
	}

	ps.tablesLock.RLock()
	if !event.Backup {
		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {

				dialAddr := route.addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Backup:        event.Backup,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr: dialAddr,
					event:    newE,
				}
			}
		}
	} else {
		ps.upBackLock.RLock()
		if _, ok := ps.myBackupsFilters[event.OriginalRoute]; !ok {
			return &pb.Ack{State: false, Info: "cannot backup"}, nil
		}

		for next, route := range ps.myBackupsFilters[event.OriginalRoute].routes {
			if route.IsInterested(p) {

				dialAddr := route.addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Backup:        false,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: newE}
			}
		}
		ps.upBackLock.RUnlock()
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if dialAddr == ps.serverAddr {
		ps.Notify(ctx, event)
		return
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
		event.Backup = true
		for _, backup := range ps.currentFilterTable.routes[event.OriginalRoute].backups {

			ctxB, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if backup == ps.serverAddr {
				ps.Notify(ctxB, event)
				break
			}

			conn, err := grpc.Dial(backup, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Notify(ctxB, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// UpdateBackup sends a new filter of the filter table to the backup
func (ps *PubSub) UpdateBackup(ctx context.Context, update *pb.Update) (*pb.Ack, error) {
	fmt.Println("UpdateBackup >> " + ps.serverAddr)

	p, err := NewPredicate(update.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	ps.upBackLock.Lock()
	if _, ok := ps.myBackupsFilters[update.Sender]; !ok {
		ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
	}

	if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
		ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats(update.RouteAddr)
	}

	ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p)
	ps.upBackLock.Unlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// updateRvRegion basically sends updates rpcs
// to the closest nodes to the attribute
func (ps *PubSub) updateRvRegion(route string, info string, rvID string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, alt := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor) {

		backupAddr := ps.ipfsDHT.FindLocal(alt).Addrs[0]
		if backupAddr == nil {
			continue
		}

		altAddr := addrForPubSubServer(backupAddr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn, err := grpc.Dial(altAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)
		update := &pb.Update{
			Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
			Route:     route,
			RouteAddr: routeAddr,
			Predicate: info,
		}

		client.UpdateBackup(ctx, update)
	}

	return nil
}

// updateMyBackups basically sends updates rpcs to its backups
// to update their versions of his filter table
func (ps *PubSub) updateMyBackups(route string, info string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, addrB := range ps.myBackups {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		conn, err := grpc.Dial(addrB, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

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

// getBackups selects f backup peers for the node,
// which are the ones closer to him by ID
func (ps *PubSub) getBackups() []string {

	var backups []string

	var dialAddr string
	for _, backup := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), ps.faultToleranceFactor) {
		backupAddr := ps.ipfsDHT.FindLocal(backup).Addrs[0]
		if backupAddr == nil {
			continue
		}

		dialAddr = addrForPubSubServer(backupAddr)
		backups = append(backups, dialAddr)
	}

	return backups
}

// eraseOldFetchNewBackup rases a old backup and recruits
// and updates another to replace him
func (ps *PubSub) eraseOldFetchNewBackup(oldAddr string) {

	var refIndex int
	for i, backup := range ps.myBackups {
		if backup == oldAddr {
			refIndex = i
		}
	}

	candidate := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), ps.faultToleranceFactor+1)
	if len(candidate) != ps.faultToleranceFactor+1 {
		return
	}

	backupAddr := ps.ipfsDHT.FindLocal(candidate[ps.faultToleranceFactor]).Addrs[0]
	if backupAddr == nil {
		return
	}
	newAddr := addrForPubSubServer(backupAddr)
	ps.myBackups[refIndex] = newAddr

	updates, err := ps.filtersForBackupRefresh()
	if err != nil {
		return
	}

	ps.refreshOneBackup(newAddr, updates)
}

// BackupRefresh refreshes the filter table the backup keeps of the peer
func (ps *PubSub) BackupRefresh(stream pb.ScoutHub_BackupRefreshServer) error {
	fmt.Println("BackupRefresh >> " + ps.serverAddr)

	var i = 0
	ps.upBackLock.Lock()
	defer ps.upBackLock.Unlock()

	for {
		update, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Ack{State: true, Info: ""})
		}
		if err != nil {
			return err
		}
		if i == 0 {
			ps.myBackupsFilters[update.Sender] = nil
		}

		p, err := NewPredicate(update.Predicate, ps.maxAttributesPerPredicate)
		if err != nil {
			return err
		}

		if ps.myBackupsFilters[update.Sender] == nil {
			ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
		}

		if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
			ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats(update.RouteAddr)
		}

		ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p)
		i = 1
	}
}

// refreashBackups sends a BackupRefresh
// to  all backup nodes
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

// filtersForBackupRefresh coverts an entire filterTable into a
// sequence of updates for easier delivery via gRPC
func (ps *PubSub) filtersForBackupRefresh() ([]*pb.Update, error) {

	var updates []*pb.Update
	for route, routeS := range ps.currentFilterTable.routes {

		ps.tablesLock.RLock()
		routeAddr := ps.currentFilterTable.routes[route].addr
		ps.tablesLock.RUnlock()

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

// refreshOneBackup refreshes one of the nodes backup
func (ps *PubSub) refreshOneBackup(backup string, updates []*pb.Update) error {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	closestID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ConvertKey(rvID))
	if kb.Closer(selfID, closestID, rvID) {
		return true, ""
	}

	return false, closestID
}

// alternativesToRv checks for alternative
// ways to reach the rendevous node
func (ps *PubSub) alternativesToRv(rvID string) []string {

	var validAlt []string
	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor)

	for _, ID := range closestIDs {
		if kb.Closer(ID, selfID, rvID) {
			attrAddr := ps.ipfsDHT.FindLocal(ID).Addrs[0]
			if attrAddr != nil {
				validAlt = append(validAlt, addrForPubSubServer(attrAddr))
			}
		} else {
			validAlt = append(validAlt, ps.serverAddr)
			break
		}
	}

	return validAlt
}

// terminateService closes the PubSub service
func (ps *PubSub) TerminateService() {
	fmt.Println("Terminate: " + ps.serverAddr)
	ps.terminate <- "end"
	ps.server.Stop()
}

// processLopp processes async operations and proceeds
// to execute cyclical functions of refreshing
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.subsToForward:
			go ps.forwardSub(pid.dialAddr, pid.sub)
		case pid := <-ps.eventsToForwardUp:
			go ps.forwardEventUp(pid.dialAddr, pid.event)
		case pid := <-ps.eventsToForwardDown:
			go ps.forwardEventDown(pid.dialAddr, pid.event)
		case pid := <-ps.interestingEvents:
			ps.record.SaveReceivedEvent(pid.EventID.PublisherID, pid.BirthTime, pid.Event)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.premiumEvents:
			ps.record.SaveReceivedEvent(pid.GroupID.OwnerAddr, pid.BirthTime, pid.Event)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.advToForward:
			go ps.forwardAdvertising(pid.dialAddr, pid.adv)
		case <-ps.heartbeatTicker.C:
			for _, filters := range ps.myFilters.filters {
				for _, filter := range filters {
					ps.MySubscribe(filter.ToString())
				}
			}
			for _, g := range ps.managedGroups {
				ps.myAdvertiseGroup(g.predicate)
			}
		case <-ps.refreshTicker.C:
			ps.tablesLock.Lock()
			ps.currentFilterTable = ps.nextFilterTable
			ps.nextFilterTable = NewFilterTable(ps.ipfsDHT)
			ps.currentAdvertiseBoard = ps.nextAdvertiseBoard
			ps.nextAdvertiseBoard = nil
			ps.tablesLock.Unlock()
			ps.refreshAllBackups()
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

func (ps *PubSub) CreateMulticastGroup(pred string) error {

	p, err := NewPredicate(pred, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	ps.managedGroups = append(ps.managedGroups, NewMulticastGroup(p, ps.serverAddr, ps.maxSubsPerRegion, ps.powerSubsPoolSize))
	ps.myAdvertiseGroup(p)

	return nil
}

// myAdvertiseGroup advertise towards the overlay the
// existence of a new multicastGroup by sharing it
// with the rendezvous nodes of the Group Predicate
func (ps *PubSub) myAdvertiseGroup(pred *Predicate) error {
	fmt.Println("myAdvertiseGroup >>" + ps.serverAddr)

	for _, attr := range pred.attributes {

		groupID := &pb.MulticastGroupID{
			OwnerAddr: ps.serverAddr,
			Predicate: pred.ToString(),
		}

		advReq := &pb.AdvertRequest{
			GroupID: groupID,
			RvId:    attr.name,
		}

		isRv, nextHop := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			ps.addAdvertToBoards(advReq)
			continue
		}

		ps.tablesLock.RLock()
		dialAddr := ps.currentFilterTable.routes[peer.Encode(nextHop)].addr
		ps.tablesLock.RUnlock()
		ps.advToForward <- &ForwardAdvert{
			dialAddr: dialAddr,
			adv:      advReq,
		}
	}

	return nil
}

// AdvertiseGroup remote call used to propagate a advertisement to the rendezvous
func (ps *PubSub) AdvertiseGroup(ctx context.Context, adv *pb.AdvertRequest) (*pb.Ack, error) {
	fmt.Println("AdvertiseGroup >> " + ps.serverAddr)

	isRv, nextHop := ps.rendezvousSelfCheck(adv.RvId)
	if isRv {
		ps.addAdvertToBoards(adv)
		return &pb.Ack{State: true, Info: ""}, nil
	}

	ps.tablesLock.RLock()
	dialAddr := ps.currentFilterTable.routes[peer.Encode(nextHop)].addr
	ps.tablesLock.RUnlock()
	ps.advToForward <- &ForwardAdvert{
		dialAddr: dialAddr,
		adv:      adv,
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardAdvertising forwards a advertisement asynchronously to the rendezvous
func (ps *PubSub) forwardAdvertising(dialAddr string, adv *pb.AdvertRequest) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

			if addr == ps.serverAddr {
				client.AdvertiseGroup(ctx, adv)
				return
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			ctxBackup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.AdvertiseGroup(ctxBackup, adv)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// addAdvertToBoard adds the advertisement to both the current and next boards
func (ps *PubSub) addAdvertToBoards(adv *pb.AdvertRequest) error {

	pAdv, err := NewPredicate(adv.GroupID.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	miss := true
	ps.tablesLock.Lock()
	defer ps.tablesLock.Unlock()

	for _, a := range ps.currentAdvertiseBoard {
		pA, _ := NewPredicate(a.Predicate, ps.maxAttributesPerPredicate)
		if a.OwnerAddr == adv.GroupID.OwnerAddr && pA.Equal(pAdv) {
			miss = false
		}
	}

	if miss {
		ps.currentAdvertiseBoard = append(ps.currentAdvertiseBoard, adv.GroupID)
	}

	for _, a := range ps.nextAdvertiseBoard {
		pA, _ := NewPredicate(a.Predicate, ps.maxAttributesPerPredicate)
		if a.OwnerAddr == adv.GroupID.OwnerAddr && pA.Equal(pAdv) {
			return nil
		}
	}

	ps.nextAdvertiseBoard = append(ps.nextAdvertiseBoard, adv.GroupID)

	return nil
}

// MyGroupSearchRequest requests to the closest rendezvous of his whished
// Group predicate for MulticastGroups of his interest
func (ps *PubSub) MySearchAndPremiumSub(pred string) error {
	fmt.Println("MySearchAndPremiumSub: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now().Format(time.StampMilli)

	p, err := NewPredicate(pred, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	minID, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
	}

	isRv, _ := ps.rendezvousSelfCheck(minAttr)
	if isRv {
		for _, g := range ps.returnGroupsOfInterest(p) {
			err := ps.MyPremiumSubscribe(pred, g.OwnerAddr, g.Predicate)
			if err == nil {
				ps.record.SaveTimeToSub(start)
			}
		}
		return nil
	}

	var dialAddr string
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	if closestAddr == nil {
		return errors.New("no address for closest peer")
	} else {
		dialAddr = addrForPubSubServer(closestAddr)
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

			if addr == ps.serverAddr {
				for _, g := range ps.returnGroupsOfInterest(p) {
					err := ps.MyPremiumSubscribe(pred, g.OwnerAddr, g.Predicate)
					if err == nil {
						ps.record.SaveTimeToSub(start)
					}
				}
				return nil
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			reply, err := client.GroupSearchRequest(ctx, req)
			if err == nil {
				for _, g := range reply.Groups {
					err := ps.MyPremiumSubscribe(pred, g.OwnerAddr, g.Predicate)
					if err == nil {
						ps.record.SaveTimeToSub(start)
					}
				}
				break
			}
		}
	} else {
		for _, g := range reply.Groups {
			err := ps.MyPremiumSubscribe(pred, g.OwnerAddr, g.Predicate)
			if err == nil {
				ps.record.SaveTimeToSub(start)
			}
		}
	}

	return nil
}

// GroupSearchRequest is a piggybacked remote call that deliveres to the myGroupSearchRequest caller
// all the multicastGroups he has in his AdvertiseBoard that comply with his search predicate
func (ps *PubSub) GroupSearchRequest(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	fmt.Println("GroupSearchRequest >> " + ps.serverAddr)

	p, err := NewPredicate(req.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return nil, err
	}

	minID, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return nil, errors.New("failed to find the closest attribute Rv")
	}

	isRv, _ := ps.rendezvousSelfCheck(minAttr)
	if isRv {
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
		dialAddr = addrForPubSubServer(closestAddr)
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

			if addr == ps.serverAddr {
				var groups map[int32]*pb.MulticastGroupID = make(map[int32]*pb.MulticastGroupID)
				for i, g := range ps.returnGroupsOfInterest(p) {
					groups[int32(i)] = g
				}

				return &pb.SearchReply{Groups: groups}, nil
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			reply, err := client.GroupSearchRequest(ctx, req)
			if err == nil {
				return reply, nil
			}
		}
	} else {
		return reply, nil
	}

	return nil, errors.New("dead end search")
}

// returnGroupsOfInterest returns all the MulticastGroups of the
// advertising board that are related to a certain predicate search
func (ps *PubSub) returnGroupsOfInterest(p *Predicate) []*pb.MulticastGroupID {

	var interestGs []*pb.MulticastGroupID
	ps.tablesLock.RLock()
	for _, g := range ps.currentAdvertiseBoard {
		pG, _ := NewPredicate(g.Predicate, ps.maxAttributesPerPredicate)

		if p.SimpleAdvMatch(pG) {
			interestGs = append(interestGs, g)
		}
	}
	ps.tablesLock.RUnlock()

	return interestGs
}

// MyPremiumSubscribe is the operation a subscriber performs in order to belong to
// a certain MulticastGroup of a certain premium publisher and predicate
func (ps *PubSub) MyPremiumSubscribe(info string, pubAddr string, pubPredicate string) error {
	fmt.Println("MyPremiumSubscribe: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubP, err := NewPredicate(pubPredicate, ps.maxAttributesPerPredicate)
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
		Cap:          int32(ps.capacity / 2),
	}

	client := pb.NewScoutHubClient(conn)
	ack, err := client.PremiumSubscribe(ctx, sub)
	if ack.State && err == nil {
		subG := &SubGroupView{
			maxAttr:   ps.maxAttributesPerPredicate,
			pubAddr:   pubAddr,
			predicate: pubP,
			helping:   false,
			attrTrees: make(map[string]*RangeAttributeTree),
		}

		ps.subbedGroups = append(ps.subbedGroups, subG)
		ps.capacity = ps.capacity / 2

		return nil
	}

	return errors.New("failed my premium subscribe")
}

// PremiumSubscribe remote call used by the myPremiumSubscribe to delegate
// the premium subscription to the premium publisher to process it
func (ps *PubSub) PremiumSubscribe(ctx context.Context, sub *pb.PremiumSubscription) (*pb.Ack, error) {
	fmt.Printf("PremiumSubscribe >> " + ps.serverAddr)

	pubP, err1 := NewPredicate(sub.PubPredicate, ps.maxAttributesPerPredicate)
	if err1 != nil {
		return &pb.Ack{State: false, Info: ""}, err1
	}

	subP, err2 := NewPredicate(sub.OwnPredicate, ps.maxAttributesPerPredicate)
	if err2 != nil {
		return &pb.Ack{State: false, Info: ""}, err2
	}

	for _, mg := range ps.managedGroups {
		if mg.predicate.Equal(pubP) {
			mg.AddSubToGroup(sub.Addr, int(sub.Cap), sub.Region, subP)
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// myPremiumUnsubscribe is the operation a premium subscriber performes
// once it wants to get out of a multicastGroup
func (ps *PubSub) MyPremiumUnsubscribe(pubPred string, pubAddr string) error {
	fmt.Println("MyPremiumUnsubscribe: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubP, err := NewPredicate(pubPred, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	for i := 0; i < len(ps.subbedGroups); i++ {
		if ps.subbedGroups[i].predicate.Equal(pubP) && ps.subbedGroups[i].pubAddr == pubAddr {
			conn, err := grpc.Dial(pubAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			protoSub := &pb.PremiumSubscription{
				Region:       ps.region,
				Addr:         ps.serverAddr,
				PubPredicate: pubPred,
			}

			client := pb.NewScoutHubClient(conn)
			ack, err := client.PremiumUnsubscribe(ctx, protoSub)
			if !ack.State && err != nil {
				return errors.New("failed unsubscribing")
			}

			if i == 0 && len(ps.subbedGroups) == 1 {
				ps.subbedGroups = nil
			} else if i == 0 {
				ps.subbedGroups = ps.subbedGroups[1:]
			} else if len(ps.subbedGroups) == i+1 {
				ps.subbedGroups = ps.subbedGroups[:i]
			} else {
				ps.subbedGroups = append(ps.subbedGroups[:i], ps.subbedGroups[i+1:]...)
			}
		}
	}

	return nil
}

// PremiumUnsubscribe remote call used by the subscriber to communicate is insterest
// to unsubscribe to a multicastGroup to the premium publisher
func (ps *PubSub) PremiumUnsubscribe(ctx context.Context, sub *pb.PremiumSubscription) (*pb.Ack, error) {
	fmt.Println("PremiumUnsubscribe >> " + ps.serverAddr)

	pubP, err := NewPredicate(sub.PubPredicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: ""}, err
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

	return &pb.Ack{State: true, Info: ""}, nil
}

// MyPremiumPublish is the operation a premium publisher runs
// when he wants to publish in one of its MulticastGroups
func (ps *PubSub) MyPremiumPublish(grpPred string, event string, eventInfo string) error {
	fmt.Println("MyPremiumPublish: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gP, err1 := NewPredicate(grpPred, ps.maxAttributesPerPredicate)
	if err1 != nil {
		return err1
	}

	eP, err2 := NewPredicate(eventInfo, ps.maxAttributesPerPredicate)
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
		BirthTime: time.Now().Format(time.StampMilli),
	}

	hTotal := len(mGrp.trackHelp)
	for _, tracker := range mGrp.trackHelp {
		go sendEventToHelper(ctx, tracker, mGrp, premiumE)
	}

	var helperFailedSubs []*SubData = nil
	for i := 0; i < hTotal; i++ {
		failedOnes := <-mGrp.failedDelivery
		helperFailedSubs = append(helperFailedSubs, failedOnes...)
	}

	before := len(mGrp.helpers)
	for _, sub := range helperFailedSubs {
		mGrp.AddSubToGroup(sub.addr, sub.capacity, sub.region, sub.pred)
	}
	aux := len(mGrp.helpers) - before

	for _, sub := range append(mGrp.helpers[len(mGrp.helpers)-aux:len(mGrp.helpers)], mGrp.AddrsToPublishEvent(eP)...) {
		go func(sub *SubData) {
			goctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conn, err := grpc.Dial(sub.addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			client.PremiumPublish(goctx, premiumE)
		}(sub)

	}

	return nil
}

// sendEventToHelper delegates an event to the helper
func sendEventToHelper(ctx context.Context, tracker *HelperTracker, mGrp *MulticastGroup, premiumE *pb.PremiumEvent) {
	conn, err := grpc.Dial(tracker.helper.addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.PremiumPublish(ctx, premiumE)
	if err != nil || !ack.State {
		mGrp.failedDelivery <- mGrp.trackHelp[tracker.helper.addr].subsDelegated
		mGrp.StopDelegating(tracker, false)
	} else {
		mGrp.failedDelivery <- nil
	}
}

// PremiumPublish remote call used not only by the premium publisher to forward its events to
// the helpers and interested subs but also by the helpers to forward to their delegated subs
func (ps *PubSub) PremiumPublish(ctx context.Context, event *pb.PremiumEvent) (*pb.Ack, error) {
	fmt.Println("PremiumPublish >> " + ps.serverAddr)

	pubP, err := NewPredicate(event.GroupID.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: ""}, err
	}

	for _, sg := range ps.subbedGroups {
		if sg.predicate.Equal(pubP) {
			if sg.helping {
				eP, err := NewPredicate(event.EventPred, ps.maxAttributesPerPredicate)
				if err != nil {
					return &pb.Ack{State: false, Info: ""}, err
				}

				for _, sub := range sg.AddrsToPublishEvent(eP) {
					go func(sub *SubData) {
						goctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()

						conn, err := grpc.Dial(sub.addr, grpc.WithInsecure())
						if err != nil {
							log.Fatalf("fail to dial: %v", err)
						}
						defer conn.Close()

						client := pb.NewScoutHubClient(conn)
						client.PremiumPublish(goctx, event)
					}(sub)
				}
			}

			ps.premiumEvents <- event
			break
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// RequestHelp is the remote call the premium publisher of a MulticastGroup
// uses to a sub of his to recruit him as a helper
func (ps *PubSub) RequestHelp(ctx context.Context, req *pb.HelpRequest) (*pb.Ack, error) {
	fmt.Println("RequestHelp >> " + ps.serverAddr)

	p, err := NewPredicate(req.GroupID.Predicate, ps.maxAttributesPerPredicate)
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

	return &pb.Ack{State: true, Info: ""}, nil
}

// DelegateSubToHelper is a remote call used by the premium publisher of
// a multicast group to delegate a sub to a sub already helping him
func (ps *PubSub) DelegateSubToHelper(ctx context.Context, sub *pb.DelegateSub) (*pb.Ack, error) {
	fmt.Printf("DelegateSubToHelper: %s\n", ps.serverAddr)

	p, err := NewPredicate(sub.GroupID.Predicate, ps.maxAttributesPerPredicate)
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

	return &pb.Ack{State: true, Info: ""}, nil
}

// ++++++++++++++++++++++ Metrics Fetching plus Testing Functions ++++++++++++++++++++++

// ReturnEventStats returns the time
// it took to receive each event
func (ps *PubSub) ReturnEventStats() []int {

	return ps.record.EventStats()
}

// ReturnSubsStats returns the time it took to receive
// confirmation of subscription completion
func (ps *PubSub) ReturnSubStats() []int {

	return ps.record.timeToSub
}

// ReturnCorrectnessStats returns the number of events missing and duplicated
func (ps *PubSub) ReturnCorrectnessStats(expected []string) (int, int) {

	return ps.record.CorrectnessStats(expected)
}

// SetHasOldPeer only goal is to set peer as old in testing scenario
func (ps *PubSub) SetHasOldPeer() { /* Not relevant in this variant */ }
