package contentpubsub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	key "github.com/libp2p/go-libp2p-kbucket/keyspace"
	pb "github.com/pedroaston/contentpubsub/pb"

	"google.golang.org/grpc"
)

// FaultToleranceFactor >> number of backups (TODO)
// MaxAttributesPerSub >> maximum allowed number of attributes per predicate (TODO)
// SubRefreshRateMin >> frequency in which a subscriber needs to resub in minutes (TODO)
const (
	FaultToleranceFactor      = 3
	MaxAttributesPerPredicate = 5
	SubRefreshRateMin         = 20
)

//PubSub data structure
type PubSub struct {
	pb.UnimplementedScoutHubServer

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	interestingEvents   chan string
	subsToForward       chan *ForwardSubRequest
	eventsToForwardUp   chan *ForwardEvent
	eventsToForwardDown chan *ForwardEvent
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
		interestingEvents:   make(chan string),
		subsToForward:       make(chan *ForwardSubRequest, 2*len(filterTable.routes)),
		eventsToForwardUp:   make(chan *ForwardEvent, 2*len(filterTable.routes)),
		eventsToForwardDown: make(chan *ForwardEvent, 2*len(filterTable.routes)),
	}

	// Need to understand why this randomly gives problems
	ps.ipfsDHT = dht

	addr := ps.ipfsDHT.Host().Addrs()[0]
	aux := strings.Split(addr.String(), "/")
	dialAddr := aux[2] + ":4" + aux[4][1:]

	lis, err := net.Listen("tcp", dialAddr)
	if err != nil {
		return nil
	}

	grpcServer := grpc.NewServer()
	pb.RegisterScoutHubServer(grpcServer, ps)
	go grpcServer.Serve(lis)
	go ps.processLoop()

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
// TODO >> need to build a reliable version
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Print("Subscribe: ")
	fmt.Println(ps.ipfsDHT.PeerID())

	p, err := NewPredicate(sub.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	alreadyDone, pNew := ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
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

		subForward := &pb.Subscription{
			PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
			Predicate: sub.Predicate,
			RvId:      sub.RvId,
		}

		ps.subsToForward <- &ForwardSubRequest{dialAddr: dialAddr, sub: subForward}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// Publish is a remote function called by a external peer to send an Event upstream
// TODO >> need to build a reliable version
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

	for next, route := range ps.currentFilterTable.routes {
		if route.IsInterested(p) {
			var dialAddr string
			nextID, err := peer.Decode(next)
			if err != nil {
				return &pb.Ack{State: false, Info: "decoding failed"}, err
			}

			nextAddr := ps.ipfsDHT.FindLocal(nextID).Addrs[0]
			if nextAddr == nil {
				return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
			} else {
				aux := strings.Split(nextAddr.String(), "/")
				dialAddr = aux[2] + ":4" + aux[4][1:]
			}

			ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event}
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// Notify is a remote function called by a external peer to send an Event downstream
// TODO >> need to build a reliable version
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {

	p, err := NewPredicate(event.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event.Event
	}

	for next, route := range ps.currentFilterTable.routes {
		if route.IsInterested(p) {
			var dialAddr string
			nextID, err := peer.Decode(next)
			if err != nil {
				return &pb.Ack{State: false, Info: "decoding failed"}, err
			}

			nextAddr := ps.ipfsDHT.FindLocal(nextID).Addrs[0]
			if nextAddr == nil {
				return &pb.Ack{State: false, Info: "No address for next hop peer"}, nil
			} else {
				aux := strings.Split(nextAddr.String(), "/")
				dialAddr = aux[2] + ":4" + aux[4][1:]
			}

			ps.eventsToForwardDown <- &ForwardEvent{dialAddr: dialAddr, event: event}
		}
	}

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
		return errors.New("Failed Subscription")
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
		}

		res, _ := ps.rendezvousSelfCheck(attr.name)
		if res {
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
			fmt.Println("Error: ")
			fmt.Println(err)
			return errors.New("Failed Publishing towards: " + attr.name)
		}
	}

	return nil
}

// forwardSub is called upon finishing the processing a
// received subscription that needs forwarding
// TODO >> to complete when implementing Fault-Tolerance
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

	// Need to retry if failed
	if ack.State == false {
		fmt.Println("Retry to be implemented")
	}
}

// forwardEventUp is called upon receiving the request to keep forward a event
// towards a rendezvous by calling another publish operation towards it
// TODO >> to complete when implementing Fault-Tolerance
func (ps *PubSub) forwardEventUp(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Publish(ctx, event)

	// Need to retry if failed
	if ack.State == false {
		fmt.Println("Retry to be implemented")
	}
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
// TODO >> to complete when implementing Fault-Tolerance
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Notify(ctx, event)

	// Need to retry if failed
	if ack.State == false {
		fmt.Println("Retry to be implemented")
	}
}

// rendezvousSelfCheck evaluates if the peer is the rendezvous node
// and if not it returns the peerID of the next subscribing hop
// TODO >> not working properly (semantically)
func (ps *PubSub) rendezvousSelfCheck(rvID string) (bool, peer.ID) {

	rvAux := kb.ConvertKey(rvID)
	closestID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(rvAux))
	selfAux, err1 := ps.ipfsDHT.PeerID().MarshalBinary()
	closestAux, err2 := closestID.MarshalBinary()
	rvIDAux, err3 := peer.ID(rvAux).MarshalBinary()
	if err1 != nil {
		return false, ""
	} else if err2 != nil {
		return false, ""
	} else if err3 != nil {
		return false, ""
	}

	selfDist := key.XORKeySpace.Distance(key.XORKeySpace.Key(rvIDAux), key.XORKeySpace.Key(selfAux))
	closestDist := key.XORKeySpace.Distance(key.XORKeySpace.Key(rvIDAux), key.XORKeySpace.Key(closestAux))

	if closestDist.Cmp(selfDist) == -1 {
		return false, closestID
	}

	return true, ""
}

type ForwardSubRequest struct {
	dialAddr string
	sub      *pb.Subscription
}

type ForwardEvent struct {
	dialAddr string
	event    *pb.Event
}

// processLopp
// TODO >> must contain subs refreshing cycle
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.subsToForward:
			ps.forwardSub(pid.dialAddr, pid.sub)
		case pid := <-ps.eventsToForwardUp:
			ps.forwardEventUp(pid.dialAddr, pid.event)
		case pid := <-ps.eventsToForwardDown:
			ps.forwardEventDown(pid.dialAddr, pid.event)
		case pid := <-ps.interestingEvents:
			fmt.Println("Received: " + pid)
		}
	}
}
