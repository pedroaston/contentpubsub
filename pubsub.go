package contentpubsub

import (
	pb "github.com/pedroaston/ScoutSubs-FastDelivery/contentpubsub/pb"
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
	//TODO >> Needs to be a RPC
	incoming chan string
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

// processLopp
// TODO >> may contain subs refreshing cycle
func (pb *PubSub) processLoop() {}
