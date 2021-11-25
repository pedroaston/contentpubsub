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

// PubSub supports all the middleware logic
type PubSub struct {
	maxSubsPerRegion          int
	powerSubsPoolSize         int
	maxAttributesPerPredicate int
	timeToCheckDelivery       time.Duration
	opResendRate              time.Duration
	rpcTimeout                time.Duration
	faultToleranceFactor      int
	region                    string
	activeRedirect            bool
	activeReliability         bool
	addrOption                bool

	pb.UnimplementedScoutHubServer
	server     *grpc.Server
	serverAddr string

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	rvCache []string

	myBackups        []string
	myBackupsFilters map[string]*FilterTable

	myTrackers      map[string]*Tracker
	myETrackers     map[string]*EventLedger
	myEBackTrackers map[string]*EventLedger

	interestingEvents   chan *pb.Event
	subsToForward       chan *ForwardSubRequest
	eventsToForwardUp   chan *ForwardEvent
	eventsToForwardDown chan *ForwardEvent
	ackToSendUp         chan *AckUp
	heartbeatTicker     *time.Ticker
	refreshTicker       *time.Ticker
	eventTicker         *time.Ticker
	subTicker           *time.Ticker
	eventTickerState    bool
	subTickerState      bool
	terminate           chan string
	unconfirmedEvents   map[string]*PubEventState
	unconfirmedSubs     map[string]*SubState
	lives               int

	tablesLock *sync.RWMutex
	upBackLock *sync.RWMutex
	ackOpLock  *sync.Mutex
	ackUpLock  *sync.Mutex

	capacity              int
	managedGroups         []*MulticastGroup
	subbedGroups          []*SubGroupView
	premiumEvents         chan *pb.PremiumEvent
	currentAdvertiseBoard []*pb.MulticastGroupID
	nextAdvertiseBoard    []*pb.MulticastGroupID
	advToForward          chan *ForwardAdvert

	record   *HistoryRecord
	session  int
	eventSeq int
}

func NewPubSub(dht *kaddht.IpfsDHT, cfg *SetupPubSub) *PubSub {

	filterTable := NewFilterTable(dht, cfg.TestgroundReady)
	auxFilterTable := NewFilterTable(dht, cfg.TestgroundReady)
	mySubs := NewRouteStats("no need")

	ps := &PubSub{
		maxSubsPerRegion:          cfg.MaxSubsPerRegion,
		powerSubsPoolSize:         cfg.PowerSubsPoolSize,
		maxAttributesPerPredicate: cfg.MaxAttributesPerPredicate,
		timeToCheckDelivery:       cfg.TimeToCheckDelivery,
		faultToleranceFactor:      cfg.FaultToleranceFactor,
		opResendRate:              cfg.OpResendRate,
		region:                    cfg.Region,
		capacity:                  cfg.Capacity,
		activeRedirect:            cfg.RedirectMechanism,
		activeReliability:         cfg.ReliableMechanisms,
		addrOption:                cfg.TestgroundReady,
		rpcTimeout:                cfg.RPCTimeout,
		currentFilterTable:        filterTable,
		nextFilterTable:           auxFilterTable,
		myFilters:                 mySubs,
		ipfsDHT:                   dht,
		myBackupsFilters:          make(map[string]*FilterTable),
		myTrackers:                make(map[string]*Tracker),
		myETrackers:               make(map[string]*EventLedger),
		myEBackTrackers:           make(map[string]*EventLedger),
		unconfirmedEvents:         make(map[string]*PubEventState),
		unconfirmedSubs:           make(map[string]*SubState),
		interestingEvents:         make(chan *pb.Event, cfg.ConcurrentProcessingFactor),
		premiumEvents:             make(chan *pb.PremiumEvent, cfg.ConcurrentProcessingFactor),
		subsToForward:             make(chan *ForwardSubRequest, cfg.ConcurrentProcessingFactor),
		eventsToForwardUp:         make(chan *ForwardEvent, cfg.ConcurrentProcessingFactor),
		eventsToForwardDown:       make(chan *ForwardEvent, cfg.ConcurrentProcessingFactor),
		terminate:                 make(chan string),
		advToForward:              make(chan *ForwardAdvert, cfg.ConcurrentProcessingFactor),
		ackToSendUp:               make(chan *AckUp, cfg.ConcurrentProcessingFactor),
		heartbeatTicker:           time.NewTicker(cfg.SubRefreshRateMin),
		refreshTicker:             time.NewTicker(2 * cfg.SubRefreshRateMin),
		eventTicker:               time.NewTicker(cfg.OpResendRate),
		subTicker:                 time.NewTicker(cfg.OpResendRate),
		eventTickerState:          false,
		subTickerState:            false,
		tablesLock:                &sync.RWMutex{},
		upBackLock:                &sync.RWMutex{},
		ackOpLock:                 &sync.Mutex{},
		ackUpLock:                 &sync.Mutex{},
		record:                    NewHistoryRecord(),
		session:                   rand.Intn(9999),
		eventSeq:                  0,
		lives:                     0,
	}

	ps.myBackups = ps.getBackups()
	ps.eventTicker.Stop()
	ps.subTicker.Stop()

	dialAddr := addrForPubSubServer(ps.ipfsDHT.Host().Addrs(), ps.addrOption)
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

type PubEventState struct {
	event    *pb.Event
	aged     bool
	dialAddr string
}

type SubState struct {
	sub      *pb.Subscription
	aged     bool
	dialAddr string
	started  string
}

type ForwardSubRequest struct {
	dialAddr string
	sub      *pb.Subscription
}

type ForwardEvent struct {
	redirectOption string
	dialAddr       string
	event          *pb.Event
}

type AckUp struct {
	dialAddr string
	eventID  *pb.EventID
	peerID   string
	rvID     string
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

	_, pNew := ps.myFilters.SimpleAddSummarizedFilter(p, nil)
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
	closestAddrs := ps.ipfsDHT.FindLocal(closest).Addrs
	if closestAddrs == nil {
		return errors.New("no address for closest peer")
	} else {
		dialAddr = addrForPubSubServer(closestAddrs, ps.addrOption)
	}

	if ps.activeRedirect {
		ps.tablesLock.RLock()
		ps.currentFilterTable.addToRouteTracker(minAttr, "sub")
		ps.currentFilterTable.addToRouteTracker(minAttr, "closes")
		ps.nextFilterTable.addToRouteTracker(minAttr, "sub")
		ps.nextFilterTable.addToRouteTracker(minAttr, "closes")
		ps.tablesLock.RUnlock()
	}

	sub := &pb.Subscription{
		PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
		Predicate: info,
		RvId:      minAttr,
		Shortcut:  "!",
		SubAddr:   ps.serverAddr,
		IntAddr:   ps.serverAddr,
	}

	if ps.activeReliability {
		ps.unconfirmedSubs[sub.Predicate] = &SubState{
			sub:      sub,
			aged:     false,
			dialAddr: dialAddr,
			started:  time.Now().Format(time.StampMilli),
		}

		if !ps.subTickerState {
			ps.subTicker.Reset(ps.opResendRate)
			ps.subTickerState = true
			ps.unconfirmedSubs[sub.Predicate].aged = true
		}
	}

	ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: sub}

	fmt.Println("Done SubMy " + ps.serverAddr)

	return nil
}

// Subscribe is a remote function called by a external peer
// to send subscriptions towards the rendezvous node
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Println("Subscribe >> " + ps.serverAddr)

	p, err := NewPredicate(sub.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: "error creating predicate"}, err
	}

	var aux []string
	for _, addr := range sub.Backups {
		aux = append(aux, addr)
	}

	ps.tablesLock.Lock()
	if ps.currentFilterTable.routes[sub.PeerID] == nil {
		ps.currentFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
	} else if ps.nextFilterTable.routes[sub.PeerID] == nil {
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
	}
	ps.tablesLock.Unlock()

	fmt.Println("phase 1 >> " + ps.serverAddr)

	ps.tablesLock.RLock()
	if ps.activeRedirect {
		if sub.Shortcut == "!" {
			ps.currentFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
			ps.nextFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
		} else {
			ps.currentFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
			ps.nextFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
		}

		ps.currentFilterTable.addToRouteTracker(sub.RvId, sub.PeerID)
		ps.nextFilterTable.addToRouteTracker(sub.RvId, sub.PeerID)
	}

	fmt.Println("phase 2 >> " + ps.serverAddr)

	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p, aux)
	alreadyDone, pNew := ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p, aux)
	ps.tablesLock.RUnlock()

	fmt.Println("phase 3 >> " + ps.serverAddr)

	if alreadyDone {
		if ps.activeReliability {
			ps.sendAckOp(sub.SubAddr, "Subscribe", sub.Predicate)
		}
		fmt.Println("Done >> " + ps.serverAddr)
		return &pb.Ack{State: true, Info: ""}, nil
	} else if pNew != nil {
		sub.Predicate = pNew.ToString()
	}

	fmt.Println("phase delta >> " + ps.serverAddr)
	isRv, nextHopAddr := ps.rendezvousSelfCheck(sub.RvId)
	if !isRv && nextHopAddr != "" {
		fmt.Println("phase 4a >> " + ps.serverAddr)
		var backups map[int32]string = make(map[int32]string)
		for i, backup := range ps.myBackups {
			backups[int32(i)] = backup
		}

		subForward := &pb.Subscription{
			PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
			Predicate: sub.Predicate,
			RvId:      sub.RvId,
			Backups:   backups,
			SubAddr:   sub.SubAddr,
			IntAddr:   ps.serverAddr,
		}

		if ps.activeRedirect {
			ps.tablesLock.RLock()
			ps.currentFilterTable.redirectLock.Lock()

			if len(ps.currentFilterTable.routeTracker[sub.RvId]) >= 2 {
				subForward.Shortcut = "!"
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
				ps.subsToForward <- &ForwardSubRequest{dialAddr: nextHopAddr, sub: subForward}
			} else if sub.Shortcut != "!" {
				subForward.Shortcut = sub.Shortcut
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
				ps.subsToForward <- &ForwardSubRequest{dialAddr: nextHopAddr, sub: subForward}
			} else {
				subForward.Shortcut = ps.currentFilterTable.routes[sub.PeerID].addr
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
				ps.subsToForward <- &ForwardSubRequest{dialAddr: nextHopAddr, sub: subForward}
			}
		} else {
			ps.subsToForward <- &ForwardSubRequest{dialAddr: nextHopAddr, sub: subForward}
		}

		fmt.Println("phase 5a >> " + ps.serverAddr)

		ps.updateMyBackups(sub.PeerID, sub.Predicate)
		fmt.Println("phase 6a >> " + ps.serverAddr)
	} else if !isRv {
		fmt.Println("nooo >> " + ps.serverAddr)
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	} else {
		if ps.activeReliability {
			fmt.Println("phase 4b >> " + ps.serverAddr)
			ps.sendAckOp(sub.SubAddr, "Subscribe", sub.Predicate)
		}

		fmt.Println("phase 5b >> " + ps.serverAddr)
		ps.updateRvRegion(sub.PeerID, sub.Predicate, sub.RvId)
		fmt.Println("phase 6b >> " + ps.serverAddr)
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardSub is called once a received subscription
// still needs to be forward towards the rendevous
func (ps *PubSub) forwardSub(dialAddr string, sub *pb.Subscription) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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
			if addr == ps.serverAddr {
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Subscribe(ctxBackup, sub)
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
	fmt.Println("MyPublish: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	notSent := true
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
			AckAddr:   ps.serverAddr,
			Backup:    false,
			BirthTime: time.Now().Format(time.StampMilli),
			PubAddr:   ps.serverAddr,
		}

		isRv, nextRvHopAddr := ps.rendezvousSelfCheck(attr.name)
		eLog := make(map[string]bool)

		if isRv && notSent {

			notSent = false
			ps.tablesLock.RLock()
			for next, route := range ps.currentFilterTable.routes {
				if route.IsInterested(p) {

					eLog[next] = false
					dialAddr := route.addr
					newE := &pb.Event{
						Event:         event.Event,
						OriginalRoute: next,
						Backup:        event.Backup,
						Predicate:     event.Predicate,
						RvId:          event.RvId,
						AckAddr:       event.AckAddr,
						PubAddr:       event.PubAddr,
						EventID:       event.EventID,
						BirthTime:     event.BirthTime,
						LastHop:       event.LastHop,
					}

					if ps.activeRedirect {

						ps.currentFilterTable.redirectLock.Lock()
						ps.nextFilterTable.redirectLock.Lock()

						if ps.currentFilterTable.redirectTable[next] == nil {
							ps.currentFilterTable.redirectTable[next] = make(map[string]string)
							ps.currentFilterTable.redirectTable[next][event.RvId] = ""
							ps.nextFilterTable.redirectTable[next] = make(map[string]string)
							ps.nextFilterTable.redirectTable[next][event.RvId] = ""

							ps.eventsToForwardDown <- &ForwardEvent{
								dialAddr:       dialAddr,
								event:          newE,
								redirectOption: "",
							}
						} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
							ps.eventsToForwardDown <- &ForwardEvent{
								dialAddr:       dialAddr,
								event:          newE,
								redirectOption: "",
							}
						} else {
							ps.eventsToForwardDown <- &ForwardEvent{
								dialAddr:       dialAddr,
								event:          newE,
								redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
							}
						}

						ps.currentFilterTable.redirectLock.Unlock()
						ps.nextFilterTable.redirectLock.Unlock()

					} else {
						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr: dialAddr,
							event:    newE,
						}
					}
				}
			}
			ps.tablesLock.RUnlock()

			if ps.activeReliability && len(eLog) > 0 {
				ps.sendLogToTrackers(attr.name, eventID, eLog, event)
			}
		} else {

			if ps.activeReliability {
				eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
				ps.unconfirmedEvents[eID] = &PubEventState{event: event, aged: false, dialAddr: nextRvHopAddr}
				if !ps.eventTickerState {
					ps.eventTicker.Reset(ps.opResendRate)
					ps.eventTickerState = true
					ps.unconfirmedEvents[eID].aged = true
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
				defer cancel()

				ps.Notify(ctx, event)
			}

			ps.eventsToForwardUp <- &ForwardEvent{dialAddr: nextRvHopAddr, event: event}
		}
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

	isRv, nextRvHopAddr := ps.rendezvousSelfCheck(event.RvId)
	if !isRv && nextRvHopAddr != "" {

		if !ps.activeReliability {
			ps.Notify(ctx, event)
		}

		newE := &pb.Event{
			Event:         event.Event,
			OriginalRoute: event.OriginalRoute,
			Backup:        event.Backup,
			Predicate:     event.Predicate,
			RvId:          event.RvId,
			AckAddr:       event.AckAddr,
			PubAddr:       event.PubAddr,
			EventID:       event.EventID,
			BirthTime:     event.BirthTime,
			LastHop:       peer.Encode(ps.ipfsDHT.PeerID()),
		}

		ps.eventsToForwardUp <- &ForwardEvent{dialAddr: nextRvHopAddr, event: newE}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	} else if isRv {
		if ps.iAmRVPublish(p, event, false) != nil {
			return &pb.Ack{State: false, Info: "rendezvous role failed"}, nil
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

func (ps *PubSub) iAmRVPublish(p *Predicate, event *pb.Event, failedRv bool) error {

	eL := make(map[string]bool)

	if failedRv && ps.lives >= 1 {
		for backup := range ps.myBackupsFilters {
			backupID, _ := peer.Decode(backup)
			if kb.Closer(backupID, ps.ipfsDHT.PeerID(), event.RvId) {

				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: backup,
					Backup:        true,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       ps.serverAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
				defer cancel()

				ack, err := ps.Notify(ctx, newE)
				if err == nil && ack.State {
					eL[backup] = false
				}

				break
			}
		}
	} else if ps.lives < 1 {
		alternatives := ps.alternativesToRv(event.RvId)
		for _, addr := range alternatives {
			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.HelpNewRv(ctx, event)
			if err == nil && ack.State {
				eL[ack.Info] = false
				break
			}
		}
	}

	ps.tablesLock.RLock()
	eIDRv := fmt.Sprintf("%s%d%d", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID)
	for _, cached := range ps.rvCache {
		if eIDRv == cached {
			return nil
		}
	}

	ps.rvCache = append(ps.rvCache, eIDRv)
	event.AckAddr = ps.serverAddr

	for next, route := range ps.currentFilterTable.routes {
		if !ps.activeReliability && next == event.LastHop {
			continue
		}

		if route.IsInterested(p) {
			eL[next] = false
			dialAddr := route.addr
			newE := &pb.Event{
				Event:         event.Event,
				OriginalRoute: next,
				Backup:        event.Backup,
				Predicate:     event.Predicate,
				RvId:          event.RvId,
				AckAddr:       event.AckAddr,
				PubAddr:       event.PubAddr,
				EventID:       event.EventID,
				BirthTime:     event.BirthTime,
				LastHop:       event.LastHop,
			}

			if ps.activeRedirect {

				ps.currentFilterTable.redirectLock.Lock()
				ps.nextFilterTable.redirectLock.Lock()

				if ps.currentFilterTable.redirectTable[next] == nil {
					ps.currentFilterTable.redirectTable[next] = make(map[string]string)
					ps.currentFilterTable.redirectTable[next][event.RvId] = ""
					ps.nextFilterTable.redirectTable[next] = make(map[string]string)
					ps.nextFilterTable.redirectTable[next][event.RvId] = ""

					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          newE,
						redirectOption: "",
					}
				} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          newE,
						redirectOption: "",
					}
				} else {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          newE,
						redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
					}
				}

				ps.currentFilterTable.redirectLock.Unlock()
				ps.nextFilterTable.redirectLock.Unlock()

			} else {
				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr: dialAddr,
					event:    newE,
				}
			}
		}
	}
	ps.tablesLock.RUnlock()

	if ps.activeReliability {
		eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
		if len(eL) > 0 {
			ps.sendLogToTrackers(event.RvId, event.EventID, eL, event)
		}

		ps.sendAckOp(event.PubAddr, "Publish", eID)
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
	}

	return nil
}

func (ps *PubSub) HelpNewRv(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("HelpNewRv >> " + ps.serverAddr)

	for backup := range ps.myBackupsFilters {
		backupID, _ := peer.Decode(backup)
		if kb.Closer(backupID, ps.ipfsDHT.PeerID(), event.RvId) {
			if ps.lives < 1 {
				return &pb.Ack{State: false, Info: "new node"}, nil
			}

			newE := &pb.Event{
				Event:         event.Event,
				OriginalRoute: backup,
				Backup:        true,
				Predicate:     event.Predicate,
				RvId:          event.RvId,
				AckAddr:       event.AckAddr,
				PubAddr:       event.PubAddr,
				EventID:       event.EventID,
				BirthTime:     event.BirthTime,
				LastHop:       event.LastHop,
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			ack, err := ps.Notify(ctx, newE)
			if err == nil && ack.State {
				return &pb.Ack{State: true, Info: peer.Encode(ps.ipfsDHT.PeerID())}, nil
			}

			break
		}
	}

	return &pb.Ack{State: false, Info: "unexpected result"}, nil
}

// sendAckOp just sends an ack to the operation initiator to confirm completion
func (ps *PubSub) sendAckOp(dialAddr string, Op string, info string) {
	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	fmt.Println("sending ack op >> " + ps.serverAddr)

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	ack := &pb.Ack{
		State: true,
		Op:    Op,
		Info:  info,
	}

	client := pb.NewScoutHubClient(conn)
	client.AckOp(ctx, ack)
}

// sendAckUp sends an ack upstream to confirm event delivery
func (ps *PubSub) sendAckUp(ack *AckUp) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	eAck := &pb.EventAck{
		EventID: ack.eventID,
		PeerID:  ack.peerID,
		RvID:    ack.rvID,
	}

	if ack.dialAddr == ps.serverAddr {
		ps.AckUp(ctx, eAck)
		return
	}

	conn, err := grpc.Dial(ack.dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	client.AckUp(ctx, eAck)
}

// sendLogToTracker
func (ps *PubSub) sendLogToTrackers(attr string, eID *pb.EventID, eLog map[string]bool, e *pb.Event) {

	eIDL := fmt.Sprintf("%s%d%d", e.EventID.PublisherID, e.EventID.SessionNumber, e.EventID.SeqID)
	if ps.myTrackers[attr] != nil && ps.myTrackers[attr].leader {
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	} else if ps.myTrackers[attr] != nil {
		ps.tablesLock.Lock()
		ps.myTrackers[attr].leader = true
		ps.tablesLock.Unlock()
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	} else {
		ps.tablesLock.Lock()
		ps.myTrackers[attr] = NewTracker(true, attr, ps, ps.timeToCheckDelivery)
		ps.tablesLock.Unlock()
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	}

	for _, addr := range ps.alternativesToRv(attr) {

		if addr == ps.serverAddr {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		rm := &pb.RecruitTrackerMessage{
			RvID:   attr,
			RvAddr: ps.serverAddr,
		}

		eL := &pb.EventLog{
			RecruitMessage: rm,
			RvID:           attr,
			EventID:        eID,
			Event:          e,
			Log:            eLog,
		}

		client := pb.NewScoutHubClient(conn)
		client.LogToTracker(ctx, eL)
	}
}

// sendAckToTrackers
func (ps *PubSub) sendAckToTrackers(ack *pb.EventAck) {

	if ps.myTrackers[ack.RvID] != nil {
		ps.myTrackers[ack.RvID].addEventAck <- ack
	} else {
		ps.tablesLock.Lock()
		ps.myTrackers[ack.RvID] = NewTracker(true, ack.RvID, ps, ps.timeToCheckDelivery)
		ps.tablesLock.Unlock()
		ps.myTrackers[ack.RvID].addEventAck <- ack
	}

	for _, addr := range ps.alternativesToRv(ack.RvID) {

		if addr == ps.serverAddr {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		rm := &pb.RecruitTrackerMessage{
			RvID:   ack.RvID,
			RvAddr: ps.serverAddr,
		}

		ack.RecruitMessage = rm

		client := pb.NewScoutHubClient(conn)
		client.AckToTracker(ctx, ack)
	}
}

// AckUp processes an event ackknowledge and if it was the last
// missing ack returns its own acknowledge upstream
func (ps *PubSub) AckUp(ctx context.Context, ack *pb.EventAck) (*pb.Ack, error) {
	fmt.Println("AckUp >> " + ps.serverAddr)

	eID := fmt.Sprintf("%s%d%d%s", ack.EventID.PublisherID, ack.EventID.SessionNumber, ack.EventID.SeqID, ack.RvID)
	isRv, _ := ps.rendezvousSelfCheck(ack.RvID)
	if ps.myEBackTrackers[eID] != nil {

		ps.ackUpLock.Lock()
		if ps.myEBackTrackers[eID] == nil {
			return &pb.Ack{State: true, Info: "Event Tracker already closed"}, nil
		}

		ps.myEBackTrackers[eID].eventLog[ack.PeerID] = true
		ps.myEBackTrackers[eID].receivedAcks++

		if ps.myEBackTrackers[eID].receivedAcks == ps.myEBackTrackers[eID].expectedAcks {
			ps.ackToSendUp <- &AckUp{dialAddr: ps.myEBackTrackers[eID].addrToAck, eventID: ack.EventID,
				peerID: ps.myEBackTrackers[eID].originalDestination, rvID: ack.RvID}
			delete(ps.myEBackTrackers, eID)
		}
		ps.ackUpLock.Unlock()

	} else if isRv {
		ps.sendAckToTrackers(ack)
	} else {

		ps.ackUpLock.Lock()
		if ps.myETrackers[eID] == nil {
			return &pb.Ack{State: true, Info: "Event Tracker already closed"}, nil
		}

		ps.myETrackers[eID].eventLog[ack.PeerID] = true
		ps.myETrackers[eID].receivedAcks++

		if ps.myETrackers[eID].receivedAcks == ps.myETrackers[eID].expectedAcks {

			ps.ackToSendUp <- &AckUp{dialAddr: ps.myETrackers[eID].addrToAck, eventID: ack.EventID,
				peerID: ps.myETrackers[eID].originalDestination, rvID: ack.RvID}

			delete(ps.myETrackers, eID)
		}
		ps.ackUpLock.Unlock()

	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// AckOp receives confirmation of a Operation and stops its resending from happening
func (ps *PubSub) AckOp(ctx context.Context, ack *pb.Ack) (*pb.Ack, error) {
	fmt.Println("AckOp >> " + ps.serverAddr)

	ps.ackOpLock.Lock()
	if ack.Op == "Publish" {
		if ps.unconfirmedEvents[ack.Info] != nil {
			delete(ps.unconfirmedEvents, ack.Info)
		}

		if len(ps.unconfirmedEvents) == 0 {
			ps.eventTicker.Stop()
			ps.eventTickerState = false
		}
	} else if ack.Op == "Subscribe" {
		if ps.unconfirmedSubs[ack.Info] != nil {
			ps.record.SaveTimeToSub(ps.unconfirmedSubs[ack.Info].started)
			delete(ps.unconfirmedSubs, ack.Info)
		}

		if len(ps.unconfirmedSubs) == 0 {
			ps.subTicker.Stop()
			ps.subTickerState = false
		}
	}
	ps.ackOpLock.Unlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// LogToTracker is the remote call a tracker receives from
// the Rv node with a event log for him to start tracking
func (ps *PubSub) LogToTracker(ctx context.Context, log *pb.EventLog) (*pb.Ack, error) {
	fmt.Println("LogTracker >> " + ps.serverAddr)

	if ps.myTrackers[log.RecruitMessage.RvID] == nil {

		ps.myTrackers[log.RecruitMessage.RvID] = NewTracker(false, log.RecruitMessage.RvID,
			ps, ps.timeToCheckDelivery)

		for _, addr := range ps.alternativesToRv(log.RvID) {

			if addr == ps.serverAddr {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}
			defer conn.Close()

			log.RecruitMessage.RvAddr = ps.serverAddr

			client := pb.NewScoutHubClient(conn)
			resp, err := client.TrackerRefresh(ctx, log.RecruitMessage)
			if err == nil && resp.State {
				break
			}
		}
	}

	eID := fmt.Sprintf("%s%d%d", log.EventID.PublisherID, log.EventID.SessionNumber, log.EventID.SeqID)
	ps.myTrackers[log.RvID].newEventToCheck(NewEventLedger(eID, log.Log, "", log.Event, ""))

	return &pb.Ack{State: true, Info: ""}, nil
}

// AckToTracker is the remote call the Rv node uses to communicate
// received event acknowledges to the tracker
func (ps *PubSub) AckToTracker(ctx context.Context, ack *pb.EventAck) (*pb.Ack, error) {
	fmt.Println("AckToTracker >> " + ps.serverAddr)

	if ps.myTrackers[ack.RecruitMessage.RvID] == nil {

		ps.myTrackers[ack.RecruitMessage.RvID] = NewTracker(false, ack.RecruitMessage.RvID,
			ps, ps.timeToCheckDelivery)

		for _, addr := range ps.alternativesToRv(ack.RecruitMessage.RvID) {

			if addr == ps.serverAddr {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}
			defer conn.Close()

			ack.RecruitMessage.RvAddr = ps.serverAddr

			client := pb.NewScoutHubClient(conn)
			resp, err := client.TrackerRefresh(ctx, ack.RecruitMessage)
			if err == nil && resp.State {
				break
			}
		}
	}

	if ps.myTrackers[ack.RvID] != nil {
		ps.myTrackers[ack.RvID].addEventAck <- ack
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// TrackerRefresh is a rpc that is requested by a new tracker to the rv
// neighbourhood in order to refresh himself with their event ledgers
func (ps *PubSub) TrackerRefresh(ctx context.Context, req *pb.RecruitTrackerMessage) (*pb.Ack, error) {
	fmt.Println("TrackerRefresh >> " + ps.serverAddr)

	if ps.myTrackers[req.RvID] != nil && len(ps.myTrackers[req.RvID].eventStats) > 0 {

		conn, err := grpc.Dial(req.RvAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		for _, l := range ps.myTrackers[req.RvID].eventStats {
			eL := &pb.EventLog{
				RecruitMessage: req,
				RvID:           l.event.RvId,
				EventID:        l.event.EventID,
				Event:          l.event,
				Log:            l.eventLog,
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			client := pb.NewScoutHubClient(conn)
			client.LogToTracker(ctx, eL)
		}
	} else {
		return &pb.Ack{State: false, Info: "no tracker"}, nil
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// resendEvent
func (ps *PubSub) resendEvent(eLog *pb.EventLog) {

	ps.tablesLock.RLock()
	defer ps.tablesLock.RUnlock()

	for p := range eLog.Log {

		dialAddr := ps.currentFilterTable.routes[p].addr
		newE := &pb.Event{
			Event:         eLog.Event.Event,
			OriginalRoute: p,
			Backup:        eLog.Event.Backup,
			Predicate:     eLog.Event.Predicate,
			RvId:          eLog.Event.RvId,
			AckAddr:       eLog.Event.AckAddr,
			PubAddr:       eLog.Event.PubAddr,
			EventID:       eLog.Event.EventID,
			BirthTime:     eLog.Event.BirthTime,
			LastHop:       eLog.Event.LastHop,
		}

		if ps.activeRedirect {

			ps.currentFilterTable.redirectLock.Lock()
			ps.nextFilterTable.redirectLock.Lock()

			if ps.currentFilterTable.redirectTable[p][eLog.Event.RvId] != "" {
				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr:       dialAddr,
					event:          newE,
					redirectOption: ps.currentFilterTable.redirectTable[p][eLog.Event.RvId],
				}
			} else {
				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr:       dialAddr,
					event:          newE,
					redirectOption: "",
				}
			}

			ps.currentFilterTable.redirectLock.Unlock()
			ps.nextFilterTable.redirectLock.Unlock()

		} else {

			ps.eventsToForwardDown <- &ForwardEvent{
				dialAddr: dialAddr,
				event:    newE,
			}

		}
	}
}

// forwardEventUp is called upon receiving the request to keep forward a event
// towards a rendezvous by calling another publish operation towards it
func (ps *PubSub) forwardEventUp(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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

			if addr == ps.serverAddr {
				p, _ := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
				ps.iAmRVPublish(p, event, true)
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Publish(ctxBackup, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// Notify is a remote function called by a external peer to send an Event downstream
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("Notify >> " + ps.serverAddr)
	ps.record.operationHistory["Notify"]++

	p, err := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if !event.Backup {
		for _, attr := range p.attributes {
			isRv, _ := ps.rendezvousSelfCheck(attr.name)
			if isRv {
				return &pb.Ack{State: true, Info: ""}, nil
			}
		}
	}

	originalDestination := event.OriginalRoute

	eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
	if ps.activeReliability && ps.myETrackers[eID] != nil {

		for node, received := range ps.myETrackers[eID].eventLog {
			if !received {

				dialAddr := ps.currentFilterTable.routes[node].addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: node,
					Backup:        event.Backup,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				if ps.activeRedirect {
					ps.currentFilterTable.redirectLock.Lock()
					ps.nextFilterTable.redirectLock.Lock()

					if ps.currentFilterTable.redirectTable[node][event.RvId] != "" {
						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr:       dialAddr,
							event:          newE,
							redirectOption: ps.currentFilterTable.redirectTable[node][event.RvId],
						}
					} else {
						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr:       dialAddr,
							event:          newE,
							redirectOption: "",
						}
					}

					ps.currentFilterTable.redirectLock.Unlock()
					ps.nextFilterTable.redirectLock.Unlock()
				} else {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr: dialAddr,
						event:    newE,
					}
				}
			}
		}

		return &pb.Ack{State: true, Info: ""}, nil
	}

	ackAddr := event.AckAddr
	event.AckAddr = ps.serverAddr
	eL := make(map[string]bool)

	ps.tablesLock.RLock()
	if !event.Backup {

		if ps.myFilters.IsInterested(p) {
			ps.interestingEvents <- event
		}

		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {

				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				eL[next] = false
				dialAddr := route.addr

				if ps.activeRedirect {
					ps.currentFilterTable.redirectLock.Lock()
					ps.nextFilterTable.redirectLock.Lock()

					if ps.currentFilterTable.redirectTable[next] == nil {
						ps.currentFilterTable.redirectTable[next] = make(map[string]string)
						ps.currentFilterTable.redirectTable[next][event.RvId] = ""
						ps.nextFilterTable.redirectTable[next] = make(map[string]string)
						ps.nextFilterTable.redirectTable[next][event.RvId] = ""

						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr:       dialAddr,
							event:          newE,
							redirectOption: "",
						}
					} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr:       dialAddr,
							event:          newE,
							redirectOption: "",
						}
					} else {
						ps.eventsToForwardDown <- &ForwardEvent{
							dialAddr:       dialAddr,
							event:          newE,
							redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
						}
					}

					ps.currentFilterTable.redirectLock.Unlock()
					ps.nextFilterTable.redirectLock.Unlock()

				} else {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr: dialAddr,
						event:    newE,
					}
				}
			}
		}

		if ps.activeReliability {
			if len(eL) > 0 && ps.myETrackers[eID] == nil {
				ps.myETrackers[eID] = NewEventLedger(eID, eL, ackAddr, event, originalDestination)
			} else {
				ps.ackToSendUp <- &AckUp{dialAddr: ackAddr, eventID: event.EventID, peerID: originalDestination, rvID: event.RvId}
			}
		}

	} else {
		ps.upBackLock.RLock()
		if _, ok := ps.myBackupsFilters[event.OriginalRoute]; !ok {
			return &pb.Ack{State: false, Info: "cannot backup"}, nil
		}

		for next, route := range ps.myBackupsFilters[event.OriginalRoute].routes {
			if route.IsInterested(p) {

				eL[next] = false
				dialAddr := route.addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Backup:        false,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: newE}
			}
		}
		ps.upBackLock.RUnlock()

		if ps.activeReliability {
			if len(eL) > 0 && ps.myEBackTrackers[eID] == nil {
				ps.myEBackTrackers[eID] = NewEventLedger(eID, eL, ackAddr, event, originalDestination)
			} else {
				ps.ackToSendUp <- &AckUp{dialAddr: ackAddr, eventID: event.EventID, peerID: originalDestination, rvID: event.RvId}
			}
		}
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event, redirect string) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	if dialAddr == ps.serverAddr {
		ps.Notify(ctx, event)
		return
	}

	if redirect != "" && ps.tryRedirect(ctx, redirect, event) {
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

			if backup == ps.serverAddr {
				ps.Notify(ctx, event)
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(backup, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Notify(ctxBackup, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// tryRedirect tries to use the redirect option to evade unnecessary downstream hops
func (ps *PubSub) tryRedirect(ctx context.Context, redirect string, event *pb.Event) bool {

	conn, err := grpc.Dial(redirect, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Notify(ctx, event)
	if err == nil && ack.State {
		return true
	}

	return false
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
	ps.upBackLock.Unlock()

	ps.upBackLock.RLock()
	ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p, nil)
	ps.upBackLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// updateMyBackups basically sends updates rpcs to its backups
// to update their versions of his filter table
func (ps *PubSub) updateMyBackups(route string, info string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, addrB := range ps.myBackups {
		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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

// updateRvRegion basically sends updates rpcs
// to the closest nodes to the attribute
func (ps *PubSub) updateRvRegion(route string, info string, rvID string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, alt := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor) {

		backupAddrs := ps.ipfsDHT.FindLocal(alt).Addrs
		if backupAddrs == nil {
			continue
		}

		altAddr := addrForPubSubServer(backupAddrs, ps.addrOption)

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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

// getBackups selects f backup peers for the node,
// which are the ones closer to him by ID
func (ps *PubSub) getBackups() []string {

	var backups []string
	var dialAddr string
	for _, backup := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), ps.faultToleranceFactor) {
		backupAddrs := ps.ipfsDHT.FindLocal(backup).Addrs
		if backupAddrs == nil {
			continue
		}

		dialAddr = addrForPubSubServer(backupAddrs, ps.addrOption)
		backups = append(backups, dialAddr)
	}

	return backups
}

// eraseOldFetchNewBackup rases a old backup and
// recruits and updates another to replace it
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

	backupAddrs := ps.ipfsDHT.FindLocal(candidate[ps.faultToleranceFactor]).Addrs
	if backupAddrs == nil {
		return
	}
	newAddr := addrForPubSubServer(backupAddrs, ps.addrOption)
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

		ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p, nil)
		i = 1
	}
}

// refreashBackups sends a BackupRefresh
// to all backup nodes
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
	ps.tablesLock.RLock()
	for route, routeS := range ps.currentFilterTable.routes {

		routeAddr := routeS.addr
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
	ps.tablesLock.RUnlock()

	return updates, nil
}

// refreshOneBackup refreshes one of the nodes backup
func (ps *PubSub) refreshOneBackup(backup string, updates []*pb.Update) error {

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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
func (ps *PubSub) rendezvousSelfCheck(rvID string) (bool, string) {

	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor+1)

	for _, closestID := range closestIDs {
		addr := addrForPubSubServer(ps.ipfsDHT.FindLocal(closestID).Addrs, ps.addrOption)
		if kb.Closer(selfID, closestID, rvID) {
			return true, ""
		} else if addr != "" {
			return false, addr
		}
	}

	return true, ""
}

// alternativesToRv checks for alternative
// ways to reach the rendevous node
func (ps *PubSub) alternativesToRv(rvID string) []string {

	var validAlt []string
	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor)

	for _, ID := range closestIDs {
		if kb.Closer(ID, selfID, rvID) {
			attrAddrs := ps.ipfsDHT.FindLocal(ID).Addrs
			if attrAddrs != nil {
				validAlt = append(validAlt, addrForPubSubServer(attrAddrs, ps.addrOption))
			}
		} else {
			validAlt = append(validAlt, ps.serverAddr)
			break
		}
	}

	return validAlt
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

// TerminateService closes the PubSub service
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
			go ps.forwardEventDown(pid.dialAddr, pid.event, pid.redirectOption)
		case pid := <-ps.interestingEvents:
			ps.record.SaveReceivedEvent(pid.EventID.PublisherID, pid.BirthTime, pid.Event)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.premiumEvents:
			ps.record.SaveReceivedEvent(pid.GroupID.OwnerAddr, pid.BirthTime, pid.Event)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.ackToSendUp:
			go ps.sendAckUp(pid)
		case pid := <-ps.advToForward:
			go ps.forwardAdvertising(pid.dialAddr, pid.adv)
		case <-ps.eventTicker.C:
			for _, e := range ps.unconfirmedEvents {
				if e.aged {
					ps.forwardEventUp(e.dialAddr, e.event)
				} else {
					e.aged = true
				}
			}
		case <-ps.subTicker.C:
			for _, sub := range ps.unconfirmedSubs {
				if sub.aged {
					ps.forwardSub(sub.dialAddr, sub.sub)
				} else {
					sub.aged = true
				}
			}
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
			ps.nextFilterTable = NewFilterTable(ps.ipfsDHT, ps.addrOption)
			ps.currentAdvertiseBoard = ps.nextAdvertiseBoard
			ps.nextAdvertiseBoard = nil
			ps.tablesLock.Unlock()
			ps.refreshAllBackups()
			ps.lives++
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

		isRv, nextRvHopAddr := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			ps.addAdvertToBoards(advReq)
			continue
		}

		ps.advToForward <- &ForwardAdvert{
			dialAddr: nextRvHopAddr,
			adv:      advReq,
		}
	}

	return nil
}

// AdvertiseGroup remote call used to propagate a advertisement to the rendezvous
func (ps *PubSub) AdvertiseGroup(ctx context.Context, adv *pb.AdvertRequest) (*pb.Ack, error) {
	fmt.Println("AdvertiseGroup >> " + ps.serverAddr)

	isRv, nextRvHopAddr := ps.rendezvousSelfCheck(adv.RvId)
	if isRv {
		ps.addAdvertToBoards(adv)
		return &pb.Ack{State: true, Info: ""}, nil
	}

	ps.advToForward <- &ForwardAdvert{
		dialAddr: nextRvHopAddr,
		adv:      adv,
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardAdvertising forwards a advertisement asynchronously to the rendezvous
func (ps *PubSub) forwardAdvertising(dialAddr string, adv *pb.AdvertRequest) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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
				ps.addAdvertToBoards(adv)
				return
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
	defer cancel()

	start := time.Now().Format(time.StampMilli)

	p, err := NewPredicate(pred, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	_, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
	}

	isRv, nextRvHopAddr := ps.rendezvousSelfCheck(minAttr)
	if isRv {
		for _, g := range ps.returnGroupsOfInterest(p) {
			err := ps.MyPremiumSubscribe(pred, g.OwnerAddr, g.Predicate)
			if err == nil {
				ps.record.SaveTimeToSub(start)
			}
		}
		return nil
	}

	conn, err := grpc.Dial(nextRvHopAddr, grpc.WithInsecure())
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

	isRv, nextRvHopAddr := ps.rendezvousSelfCheck(req.RvID)
	if isRv {
		var groups map[int32]*pb.MulticastGroupID = make(map[int32]*pb.MulticastGroupID)
		for i, g := range ps.returnGroupsOfInterest(p) {
			groups[int32(i)] = g
		}

		return &pb.SearchReply{Groups: groups}, nil
	}

	conn, err := grpc.Dial(nextRvHopAddr, grpc.WithInsecure())
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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
			goctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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
						goctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
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

	return ps.record.SubStats()
}

// ReturnCorrectnessStats returns the number of events missing and duplicated
func (ps *PubSub) ReturnCorrectnessStats(expected []string) (int, int) {

	return ps.record.CorrectnessStats(expected)
}

// ReturnOpStats returns the number of times a operation was executed
func (ps *PubSub) ReturnOpStats(opName string) int {

	return ps.record.operationHistory[opName]
}

// SetHasOldPeer only goal is to set peer as old in testing scenario
func (ps *PubSub) SetHasOldPeer() {

	ps.lives = 100
}
