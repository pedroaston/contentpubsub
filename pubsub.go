package contentpubsub

import (
	"context"
	"log"
	"math/big"
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
	}

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) *pb.Ack {

	p, err := NewPredicate(sub.Predicate)

	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}
	}

	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)
	ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p)

	// TODO >> Verify if is the rendezvous to continue sending sub or not

	return &pb.Ack{State: true, Info: ""}
}

// Publish is a remote function called by a external peer to send an Event upstream
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Publish(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// Notify is a remote function called by a external peer to send an Event downstream
// TODO >> need to build a unreliable version first
// INCOMPLETE
func (ps *PubSub) Notify(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// MySubscribe
// INCOMPLETE
// Possible Whishlist!
// 1 >> verify redundancy when creating a sub
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

		if candidateDist.Cmp(minDist) == -1 || minDist == nil {
			minID = candidateID
			minDist = candidateDist
		}
	}

	// TODO >> Chosing the closest peer to
	closest := ps.ipfsDHT.RoutingTable().NearestPeer(kb.ID(minID))
	closestAddr := ps.ipfsDHT.FindLocal(closest).Addrs[0]

	if closestAddr == nil {
		// TODO >> need to create a error implementation
		return err
	}

	// TODO >> Sending subscription
	conn, err := grpc.Dial(closestAddr.String())
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

	if !ack.State || err != nil {
		// TODO >> need to create a error implementation
		return err
	}

	return nil
}

// processLopp
// TODO >> may contain subs refreshing cycle
func (pb *PubSub) processLoop() {}
