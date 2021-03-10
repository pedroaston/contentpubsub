package contentpubsub

type PubSub struct {
	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
}

func NewPubSub() *PubSub {

	ps := &PubSub{
		currentFilterTable: NewFilterTable(),
		nextFilterTable:    NewFilterTable(),
	}

	return ps
}
