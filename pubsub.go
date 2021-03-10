package contentpubsub

//PubSub's data structure
type PubSub struct {
	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	//TODO >> Needs to be a RPC
	incoming chan string
}

//Initialize the PubSub's data structure
func NewPubSub() *PubSub {

	filterTable := NewFilterTable()

	ps := &PubSub{
		currentFilterTable: filterTable,
		nextFilterTable:    filterTable,
	}

	return ps
}

func (pb *PubSub) processLoop() {
	//TODO
}
