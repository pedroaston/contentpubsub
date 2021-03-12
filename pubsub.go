package contentpubsub

// FaultToleranceFactor >> number of backups
// MaxAttributesPerSub >> maximum allowed number of attributes per predicate
// SubRefreshRateMin >> frequency in which a subscriber needs to resub in minutes
const (
	FaultToleranceFactor = 3
	MaxAttributesPerSub  = 5
	SubRefreshRateMin    = 20
)

//PubSub data structure
type PubSub struct {
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
