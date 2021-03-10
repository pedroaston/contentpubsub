package contentpubsub

type RouteStats struct {
	filters map[string]*Predicate
}

type FilterTable struct {
	routes map[string]*RouteStats
}

func NewFilterTable() *FilterTable {

	ft := &FilterTable{
		routes: make(map[string]*RouteStats),
	}

	return ft
}
