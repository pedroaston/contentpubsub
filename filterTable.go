package contentpubsub

// RouteStats keeps filters for each pubsub peer
// it is connected and its backups in case of
// peer failure
type RouteStats struct {
	filters map[string]*Predicate
	backups [FaultToleranceFactor]string
}

// FilterTable keeps filter information of all peers
type FilterTable struct {
	routes map[string]*RouteStats
}

// NewFilterTable initializes a FilterTable
// TODO >> needs to get info from kad-dht
func NewFilterTable() *FilterTable {

	ft := &FilterTable{
		routes: make(map[string]*RouteStats),
	}

	return ft
}
