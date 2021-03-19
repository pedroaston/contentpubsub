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

	dht "github.com/libp2p/go-libp2p-kad-dht"
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

	ipfsDHT *dht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	mySubs             []*Predicate

	subsToForward chan *ForwardSubRequest
}

// NewPubSub initializes the PubSub's data structure
// setup the server and starts processloop
func NewPubSub(dht *dht.IpfsDHT) *PubSub {

	filterTable := NewFilterTable(dht)
	ps := &PubSub{
		currentFilterTable: filterTable,
		nextFilterTable:    filterTable,
		ipfsDHT:            dht,
		subsToForward:      make(chan *ForwardSubRequest),
	}

	// Start Server
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

	// Start processloop
	go ps.processLoop()

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
// TODO >> need to build a unreliable version first
// TODO >> in the merge case we need to forward the new filter
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Print("Subscribe: ")
	fmt.Println(ps.ipfsDHT.PeerID())
	p, err := NewPredicate(sub.Predicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	// modify this process to stop or send a different filter
	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	alreadyDone := ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	if alreadyDone {
		return &pb.Ack{State: true, Info: ""}, nil
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
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Publish(ctx context.Context, sub *pb.Event) (*pb.Ack, error) {

	return &pb.Ack{State: true, Info: ""}, nil
}

// Notify is a remote function called by a external peer to send an Event downstream
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Notify(ctx context.Context, sub *pb.Event) (*pb.Ack, error) {

	return &pb.Ack{State: true, Info: ""}, nil
}

// MySubscribe
// TODO list!
// 1 >> verify redundancy when appending a sub
func (ps *PubSub) MySubscribe(info string) error {
	fmt.Print("MySubscribe: ")
	fmt.Println(ps.ipfsDHT.PeerID())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(info)
	if err != nil {
		return err
	}

	ps.mySubs = append(ps.mySubs, p)

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
		PeerID:    peer.Encode(ps.ipfsDHT.Host().ID()),
		Predicate: info,
		RvId:      minAttr,
	}

	ack, err := client.Subscribe(ctx, sub)
	if err != nil || !ack.State {
		return errors.New("Failed Subscription")
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

// rendezvousSelfCheck evaluates if the peer is the
// rendezvous node and if not it returns the peerID
// of the next subscribing hop
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

// processLopp
// TODO >> may contain subs refreshing cycle
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.subsToForward:
			ps.forwardSub(pid.dialAddr, pid.sub)
		}
	}
}
