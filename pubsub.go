package contentpubsub

import (
	"context"
	"errors"
	"log"
	"math/big"
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
}

// NewPubSub initializes the PubSub's data structure
func NewPubSub(dht *dht.IpfsDHT) *PubSub {

	filterTable := NewFilterTable(dht)

	ps := &PubSub{
		currentFilterTable: filterTable,
		nextFilterTable:    filterTable,
		ipfsDHT:            dht,
	}

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {

	p, err := NewPredicate(sub.Predicate)

	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)

	// TODO >> Verify if is the rendezvous to continue sending sub or not

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
// INCOMPLETE
// TODO list!
// 1 >> verify redundancy when creating a sub
// 2 >> verify is subscriber is the rendezvous (test if that can happen)
func (ps *PubSub) MySubscribe(info string) error {
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
			minID = candidateID
			minDist = candidateDist
		}
	}

	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]
	var dialAddr string

	if closestAddr == nil {
		return errors.New("No address for closest peer")
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

	sub := &pb.Subscription{
		PeerID:    ps.ipfsDHT.Host().ID().Pretty(),
		Predicate: info,
		RvId:      minID.Pretty(),
	}

	ack, err := client.Subscribe(ctx, sub)

	if err != nil || !ack.State {
		return errors.New("Failed Subscription")
	}

	return nil
}

// rendezvousSelfCheck evaluates if the peer is the
// rendezvous node and if not it returns the peerID
// of the next subscribing hop
func (ps *PubSub) rendezvousSelfCheck(rvID string) (bool, peer.ID) {

	closestID := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(rvID))
	selfAux, err1 := ps.ipfsDHT.PeerID().MarshalBinary()
	closestAux, err2 := closestID.MarshalBinary()
	rvAux, err3 := peer.IDFromString(rvID)
	rvIDAux, err4 := rvAux.MarshalBinary()

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return false, ""
	}

	selfDist := key.XORKeySpace.Distance(key.XORKeySpace.Key(rvIDAux), key.XORKeySpace.Key(selfAux))
	closestDist := key.XORKeySpace.Distance(key.XORKeySpace.Key(rvIDAux), key.XORKeySpace.Key(closestAux))

	if closestDist.Cmp(selfDist) == -1 {
		return false, closestID
	} else {
		return true, ""
	}
}

// processLopp
// TODO >> may contain subs refreshing cycle
func (pb *PubSub) processLoop() {}
