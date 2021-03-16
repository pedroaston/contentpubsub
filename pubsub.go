package contentpubsub

import (
	"context"
	"log"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	pb "github.com/pedroaston/contentpubsub/pb"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/internal/errors"
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
func (ps *PubSub) Publish(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// Notify is a remote function called by a external peer to send an Event downstream
// TODO >> need to build a unreliable version first
func (ps *PubSub) Notify(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// MySubscribe
// TODO >> incomplete
func (ps *PubSub) MySubscribe(info string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	p, err := NewPredicate(info)

	if err != nil {
		return err
	}

	ps.mySubs = append(ps.mySubs, p)
	var rvCandidates map[peer.ID]int

	for _, attr := range p.attributes {
		rvCandidates[peer.ID(kb.ConvertKey(attr.name))] = 0
	}

	conn, err := grpc.Dial("localhost:666")
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewScoutHubClient(conn)

	sub := &pb.Subscription{
		PeerID:    ps.ipfsDHT.Host().ID().Pretty(),
		Predicate: info,
		RvId:      "TODO",
	}

	ack, err := client.Subscribe(ctx, sub)

	if !ack.State || err != nil {
		return errors.New("Unsuccessful subscription")
	}

	return nil
}

// processLopp
// TODO >> may contain subs refreshing cycle
func (pb *PubSub) processLoop() {}
