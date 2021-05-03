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
	heartbeatTicker     *time.Ticker
	refreshTicker       *time.Ticker
	terminate           chan string

	tablesLock *sync.RWMutex

	managedGroups         []*MulticastGroup
	subbedGroups          []*SubGroupView
	region                string
	subRegion             string
	premiumEvents         chan *pb.PremiumEvent
	currentAdvertiseBoard []*pb.MulticastGroupID
	nextAdvertiseBoard    []*pb.MulticastGroupID
	advToForward          chan *ForwardAdvert

	record   *HistoryRecord
	session  int
	eventSeq int
}

// NewPubSub initializes the PubSub's data structure
// sets up the server and starts processloop
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
		heartbeatTicker:     time.NewTicker(SubRefreshRateMin * time.Minute),
		refreshTicker:       time.NewTicker(2 * SubRefreshRateMin * time.Minute),
		tablesLock:          &sync.RWMutex{},
		region:              region,
		subRegion:           subRegion,
		record:              NewHistoryRecord(),
		session:             rand.Intn(9999),
		eventSeq:            0,
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
	redirectOption string
	originalRoute  string
	dialAddr       string
	event          *pb.Event
}

// MySubscribe subscribes to certain event(s) and saves
// it in myFilters for further resubing operations and
// assess if node is interested in the events it receives
func (ps *PubSub) MySubscribe(info string) error {
	fmt.Println("MySubscribe: " + ps.serverAddr)

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	_, pNew := ps.myFilters.SimpleAddSummarizedFilter(p)
	if pNew != nil {
		p = pNew
	}

	minID, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
	}

	res, _ := ps.rendezvousSelfCheck(minAttr)
	if res {
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

	ps.tablesLock.RLock()
	ps.currentFilterTable.addToRouteTracker(minAttr, "sub")
	ps.currentFilterTable.addToRouteTracker(minAttr, "closes")
	ps.nextFilterTable.addToRouteTracker(minAttr, "sub")
	ps.nextFilterTable.addToRouteTracker(minAttr, "closes")
	ps.tablesLock.RUnlock()

	sub := &pb.Subscription{
		PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
		Predicate: info,
		RvId:      minAttr,
		Shortcut:  "!",
	}

	ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: sub}

	// Statistical Code
	ps.record.AddOperationStat("mySubscribe")

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

	p, err := NewPredicate(sub.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	var aux []string
	for _, addr := range sub.Backups {
		aux = append(aux, addr)
	}

	ps.tablesLock.RLock()
	if sub.Shortcut == "!" {
		ps.currentFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
		ps.nextFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
	} else {
		ps.currentFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
		ps.nextFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
	}

	ps.currentFilterTable.addToRouteTracker(sub.RvId, sub.PeerID)
	ps.nextFilterTable.addToRouteTracker(sub.RvId, sub.PeerID)

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
		var dialAddr string
		closestAddr := ps.ipfsDHT.FindLocal(nextHop).Addrs[0]
		if closestAddr == nil {
			return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
		} else {
			dialAddr = addrForPubSubServer(closestAddr)
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

		ps.tablesLock.RLock()
		defer ps.tablesLock.RUnlock()
		ps.currentFilterTable.redirectLock.Lock()
		defer ps.currentFilterTable.redirectLock.Unlock()

		if len(ps.currentFilterTable.routeTracker[sub.RvId]) == 2 {
			subForward.Shortcut = "!"
			ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}
		} else if sub.Shortcut != "!" {
			subForward.Shortcut = sub.Shortcut
			ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}
		} else {
			var redirectAddr string
			auxID, err := peer.Decode(sub.PeerID)
			if err != nil {
				return nil, err
			}

			fetchAddr := ps.ipfsDHT.FindLocal(auxID).Addrs[0]
			if fetchAddr == nil {
				return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
			} else {
				aux := strings.Split(fetchAddr.String(), "/")
				redirectAddr = aux[2] + ":4" + aux[4][1:]
			}

			subForward.Shortcut = redirectAddr
			ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}
		}
	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	}

	// Statistical Code
	ps.record.AddOperationStat("Subscribe")

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardSub is called once a received subscription
// still needs to be forward towards the rendevous
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

// MyUnsubscribe deletes a specific predicate out of mySubs
// list which will stop the refreshing of thatsub and stop
// delivering to the user those contained events
func (ps *PubSub) MyUnsubscribe(info string) error {
	fmt.Printf("myUnsubscribe: %s\n", ps.serverAddr)

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
func (ps *PubSub) MyPublish(data string, info string) error {
	fmt.Printf("myPublish: %s\n", ps.serverAddr)

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
			BirthTime: time.Now().Format(time.StampMilli),
		}

		ps.tablesLock.RLock()
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
					dialAddr = addrForPubSubServer(nextAddr)
				}

				ps.currentFilterTable.redirectLock.Lock()
				ps.nextFilterTable.redirectLock.Lock()
				if ps.currentFilterTable.redirectTable[next] == nil {
					ps.currentFilterTable.redirectTable[next] = make(map[string]string)
					ps.currentFilterTable.redirectTable[next][event.RvId] = ""
					ps.nextFilterTable.redirectTable[next] = make(map[string]string)
					ps.nextFilterTable.redirectTable[next][event.RvId] = ""
					ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event, redirectOption: ""}
				} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
					ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event, redirectOption: ""}
				} else {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          event,
						redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
					}
				}

				ps.currentFilterTable.redirectLock.Unlock()
				ps.nextFilterTable.redirectLock.Unlock()
			}
		}
		ps.tablesLock.RUnlock()

		res, _ := ps.rendezvousSelfCheck(attr.name)
		if res {
			continue
		}

		attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(attr.name))
		attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]
		if attrAddr == nil {
			return errors.New("no address for closest peer")
		} else {
			dialAddr = addrForPubSubServer(attrAddr)
		}

		ps.eventsToForwardUp <- &ForwardEvent{dialAddr: dialAddr, event: event}
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
			dialAddr = addrForPubSubServer(nextAddr)
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
				dialAddr = addrForPubSubServer(nextAddr)
			}

			ps.currentFilterTable.redirectLock.Lock()
			ps.nextFilterTable.redirectLock.Lock()

			if ps.currentFilterTable.redirectTable[next] == nil {
				ps.currentFilterTable.redirectTable[next] = make(map[string]string)
				ps.currentFilterTable.redirectTable[next][event.RvId] = ""
				ps.nextFilterTable.redirectTable[next] = make(map[string]string)
				ps.nextFilterTable.redirectTable[next][event.RvId] = ""

				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr:       dialAddr,
					event:          event,
					redirectOption: "",
					originalRoute:  next,
				}
			} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr:       dialAddr,
					event:          event,
					redirectOption: "",
					originalRoute:  next,
				}
			} else {
				ps.eventsToForwardDown <- &ForwardEvent{
					dialAddr:       dialAddr,
					event:          event,
					redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
					originalRoute:  next,
				}
			}

			ps.currentFilterTable.redirectLock.Unlock()
			ps.nextFilterTable.redirectLock.Unlock()
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
					dialAddr = addrForPubSubServer(nextAddr)
				}

				ps.currentFilterTable.redirectLock.Lock()
				ps.nextFilterTable.redirectLock.Lock()

				if ps.currentFilterTable.redirectTable[next] == nil {
					ps.currentFilterTable.redirectTable[next] = make(map[string]string)
					ps.currentFilterTable.redirectTable[next][event.RvId] = ""
					ps.nextFilterTable.redirectTable[next] = make(map[string]string)
					ps.nextFilterTable.redirectTable[next][event.RvId] = ""

					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          event,
						redirectOption: "",
						originalRoute:  next,
					}
				} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          event,
						redirectOption: "",
						originalRoute:  next,
					}
				} else {
					ps.eventsToForwardDown <- &ForwardEvent{
						dialAddr:       dialAddr,
						event:          event,
						redirectOption: ps.currentFilterTable.redirectTable[next][event.RvId],
						originalRoute:  next,
					}
				}

				ps.currentFilterTable.redirectLock.Unlock()
				ps.nextFilterTable.redirectLock.Unlock()
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
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event, originalRoute string, redirect string) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	if dialAddr == ps.serverAddr {
		ps.Notify(ctx, event)
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

// tryRedirect
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

// getBackups selects f backup peers for the node,
// which are the ones closer to him by ID
func (ps *PubSub) getBackups() []string {

	var backups []string

	var dialAddr string
	for _, backup := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), FaultToleranceFactor) {
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

// BackupRefresh refreshes the filter table the backup keeps of the peer
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

// refreshOneBackup refreshes one of the nodes backup
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

// alternativesToRv checks for alternative
// ways to reach the rendevous node
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

// gracefullyTerminate unsubscribes before closing
func (ps *PubSub) gracefullyTerminate() {
	for _, g := range ps.subbedGroups {
		ps.MyPremiumUnsubscribe(g.predicate.ToString(), g.pubAddr)
	}

	ps.terminateService()
}

// terminateService closes the PubSub service
func (ps *PubSub) terminateService() {
	ps.terminate <- "end"
	ps.server.Stop()
	ps.ipfsDHT.Close()
}

// processLopp processes async operations and proceeds
// to execute cyclical functions of refreshing
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.subsToForward:
			ps.forwardSub(pid.dialAddr, pid.sub)
		case pid := <-ps.eventsToForwardUp:
			ps.forwardEventUp(pid.dialAddr, pid.event)
		case pid := <-ps.eventsToForwardDown:
			ps.forwardEventDown(pid.dialAddr, pid.event, pid.originalRoute, pid.redirectOption)
		case pid := <-ps.interestingEvents:
			ps.record.SaveReceivedEvent(pid)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.premiumEvents:
			ps.record.SaveReceivedPremiumEvent(pid)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case pid := <-ps.advToForward:
			ps.forwardAdvertising(pid.dialAddr, pid.adv)
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
			ps.refreshAllBackups()
			ps.tablesLock.Unlock()
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

// CreateMulticastGroup is used by a premium
// publisher to create a MulticastGroup
func (ps *PubSub) CreateMulticastGroup(pred string) error {

	p, err := NewPredicate(pred)
	if err != nil {
		return err
	}

	ps.managedGroups = append(ps.managedGroups, NewMulticastGroup(p, ps.serverAddr))
	ps.myAdvertiseGroup(p)

	return nil
}

// myAdvertiseGroup advertise towards the overlay the
// existing of a new multicastGroup by sharing it
// with rendezvous nodes of the Group Predicate
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
			ps.addAdvertToBoards(advReq)
			return nil
		}

		attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(attr.name))
		attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]
		if attrAddr == nil {
			return errors.New("no address for closest peer")
		} else {
			dialAddr = addrForPubSubServer(attrAddr)
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

// AdvertiseGroup remote call used to propagate the advertisement to the rendezvous
func (ps *PubSub) AdvertiseGroup(ctx context.Context, adv *pb.AdvertRequest) (*pb.Ack, error) {
	fmt.Printf("AdvertiseGroup: %s\n", ps.serverAddr)

	res, _ := ps.rendezvousSelfCheck(adv.RvId)
	if res {
		ps.addAdvertToBoards(adv)
		return &pb.Ack{State: true, Info: ""}, nil
	}

	attrID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(adv.RvId))
	attrAddr := ps.ipfsDHT.FindLocal(attrID).Addrs[0]

	var dialAddr string
	if attrAddr == nil {
		return nil, errors.New("no address for closest peer")
	} else {
		dialAddr = addrForPubSubServer(attrAddr)
	}

	ps.advToForward <- &ForwardAdvert{
		dialAddr: dialAddr,
		adv:      adv,
	}

	// Statistical Code
	ps.record.AddOperationStat("AdvertiseGroup")

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardAdvertising forwards the advertisement asynchronously to the rendezvous
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

// addAdvertToBoard adds the advertisement to both the current and next boards
func (ps *PubSub) addAdvertToBoards(adv *pb.AdvertRequest) error {

	pAdv, err := NewPredicate(adv.GroupID.Predicate)
	if err != nil {
		return err
	}

	miss := true
	ps.tablesLock.Lock()
	defer ps.tablesLock.Unlock()

	for _, a := range ps.currentAdvertiseBoard {
		pA, _ := NewPredicate(a.Predicate)
		if a.OwnerAddr == adv.GroupID.OwnerAddr && pA.Equal(pAdv) {
			miss = false
		}
	}

	if miss {
		ps.currentAdvertiseBoard = append(ps.currentAdvertiseBoard, adv.GroupID)
	}

	for _, a := range ps.nextAdvertiseBoard {
		pA, _ := NewPredicate(a.Predicate)
		if a.OwnerAddr == adv.GroupID.OwnerAddr && pA.Equal(pAdv) {
			return nil
		}
	}

	ps.nextAdvertiseBoard = append(ps.nextAdvertiseBoard, adv.GroupID)

	return nil
}

// MyGroupSearchRequest requests to the closest rendezvous of his whished
// Group predicate for MulticastGroups of his interest
func (ps *PubSub) MyGroupSearchRequest(pred string) error {
	fmt.Println("myGroupSearchRequest: " + ps.serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(pred)
	if err != nil {
		return err
	}

	minID, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
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

// GroupSearchRequest is a piggybacked remote call that deliveres to the myGroupSerchRequest caller
// all the multicastGroups he has in his AdvertiseBoard that comply with his search predicate
func (ps *PubSub) GroupSearchRequest(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	fmt.Println("GroupSearchRequest: " + ps.serverAddr)

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

// returnGroupsOfInterest returns all the MulticastGroups of the
// advertising board that are related to a certain predicate search
func (ps *PubSub) returnGroupsOfInterest(p *Predicate) []*pb.MulticastGroupID {

	var interestGs []*pb.MulticastGroupID
	ps.tablesLock.RLock()
	for _, g := range ps.currentAdvertiseBoard {
		pG, _ := NewPredicate(g.Predicate)
		if pG.SimplePredicateMatch(p) {
			interestGs = append(interestGs, g)
		}
	}

	ps.tablesLock.RUnlock()
	return interestGs
}

// MyPremiumSubscribe is the operation a subscriber performs in order to belong to
// a certain MulticastGroup of a certain premium publisher and predicate
func (ps *PubSub) MyPremiumSubscribe(info string, pubAddr string, pubPredicate string, cap int) error {
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

// PremiumSubscribe remote call used by the myPremiumSubscribe to delegate
// the premium subscription to the premium publisher to process it
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
			mg.AddSubToGroup(sub.Addr, int(sub.Cap), sub.Region, sub.SubRegion, subP)
		}
	}

	// Statistical Code
	ps.record.AddOperationStat("PremiumSubscribe")

	return &pb.Ack{State: true, Info: ""}, nil
}

// myPremiumUnsubscribe is the operation a premium subscriber performes
// once it wants to get out of a multicastGroup
func (ps *PubSub) MyPremiumUnsubscribe(pubPred string, pubAddr string) error {
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

// PremiumUnsubscribe remote call used by the subscriber to communicate is insterest
// to unsubscribe to a multicastGroup to the premium publisher
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

// MyPremiumPublish is the operation a premium publisher runs
// when he wants to publish in one of its MultiastGroups
func (ps *PubSub) MyPremiumPublish(grpPred string, event string, eventInfo string) error {
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
		mGrp.AddSubToGroup(sub.addr, sub.capacity, sub.region, sub.subRegion, sub.pred)
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

// RequestHelp is the remote call the premium publisher of a MulticastGroup
// uses to a sub of his to recruit him as a helper
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

// DelegateSubToHelper is a remote call used by the premium publisher of
// a multicast group to delegate a sub to a sub already helping him
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

func (ps *PubSub) ReturnReceivedEventsStats() (int, int, int, int) {

	return ps.record.CompileLatencyResults()
}
