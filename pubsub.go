package contentpubsub

import (
	"context"

	pb "github.com/pedroaston/contentpubsub/pb"
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
	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	mySubs             []*Predicate
}

// NewPubSub initializes the PubSub's data structure
func NewPubSub() *PubSub {

	filterTable := NewFilterTable()

	ps := &PubSub{
		currentFilterTable: filterTable,
		nextFilterTable:    filterTable,
	}

	return ps
}

// Subscribe is a remote function called by a external peer to send subscriptions
// TODO
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// Publish is a remote function called by a external peer to send an Event upstream
// TODO
func (ps *PubSub) Publish(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// Notify is a remote function called by a external peer to send an Event downstream
// TODO
func (ps *PubSub) Notify(ctx context.Context, sub *pb.Event) *pb.Ack {

	return &pb.Ack{State: true, Info: ""}
}

// processLopp
// TODO >> may contain subs refreshing cycle
func (pb *PubSub) processLoop() {}
