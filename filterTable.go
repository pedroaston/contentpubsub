package contentpubsub

import (
	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/libp2p/go-libp2p-core/peer"
)

// RouteStats keeps filters for each pubsub peer it is
// connected and its backups in case of failure
type RouteStats struct {
	filters map[int][]*Predicate
	backups [FaultToleranceFactor]string
}

// NewRouteStats initializes a routestat
func NewRouteStats() *RouteStats {
	r := &RouteStats{filters: make(map[int][]*Predicate)}

	return r
}

// FilterTable keeps filter information of all peers
type FilterTable struct {
	routes map[peer.ID]*RouteStats
}

// NewFilterTable initializes a FilterTable
func NewFilterTable(dht *dht.IpfsDHT) *FilterTable {

	peers := dht.RoutingTable().GetPeerInfos()

	ft := &FilterTable{
		routes: make(map[peer.ID]*RouteStats),
	}

	for _, peer := range peers {
		ft.routes[peer.Id] = NewRouteStats()
	}

	return ft
}

// SimpleAddSummarizedFilter is called upon receiving a subscription
// filter to see if it should be added if exclusive, merge
// with others or encompass or be encompassed by others
func (rs *RouteStats) SimpleAddSummarizedFilter(p *Predicate) {

	for i, filters := range rs.filters {

		if len(p.attributes) > i {
			for _, f := range filters {
				if f.SimplePredicateMatch(p) {
					return
				}
			}
		} else if len(p.attributes) == i {
			for j := 0; j < len(filters); j++ {
				if filters[j].SimplePredicateMatch(p) {
					return
				} else if p.SimplePredicateMatch(filters[j]) {
					if j == 0 {
						rs.filters[i] = nil
					} else if len(filters) == j+1 {
						rs.filters[i] = filters[:j-1]
					} else {
						rs.filters[i] = append(filters[:j-1], filters[j+1:]...)
						j--
					}
				} else if ok, pNew := filters[j].TryMergePredicates(p); ok {
					p = pNew
					if j == 0 {
						rs.filters[i] = nil
					} else if len(filters) == j+1 {
						rs.filters[i] = filters[:j-1]
					} else {
						rs.filters[i] = append(filters[:j-1], filters[j+1:]...)
						j--
					}
				}
			}
		} else {
			for j := 0; j < len(filters); j++ {
				if p.SimplePredicateMatch(filters[j]) {
					if j == 0 {
						rs.filters[i] = nil
					} else if len(filters) == j+1 {
						rs.filters[i] = filters[:j-1]
					} else {
						rs.filters[i] = append(filters[:j-1], filters[j+1:]...)
						j--
					}
				}
			}
		}
	}

	rs.filters[len(p.attributes)] = append(rs.filters[len(p.attributes)], p)
}
