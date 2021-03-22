package contentpubsub

import (
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// RouteStats keeps filters for each pubsub peer it is
// connected and its backups in case of failure
type RouteStats struct {
	filters   map[int][]*Predicate
	backups   [FaultToleranceFactor]string
	routeLock *sync.Mutex
}

// NewRouteStats initializes a routestat
func NewRouteStats() *RouteStats {
	r := &RouteStats{
		filters:   make(map[int][]*Predicate),
		routeLock: &sync.Mutex{},
	}

	return r
}

// FilterTable keeps filter information of all peers
type FilterTable struct {
	routes map[string]*RouteStats
}

// NewFilterTable initializes a FilterTable
func NewFilterTable(dht *dht.IpfsDHT) *FilterTable {

	peers := dht.RoutingTable().GetPeerInfos()

	ft := &FilterTable{
		routes: make(map[string]*RouteStats),
	}

	for _, peerStat := range peers {
		ft.routes[peer.Encode(peerStat.Id)] = NewRouteStats()
	}

	return ft
}

func (ft *FilterTable) PrintFilterTable() {

	fmt.Println("Printing Table:")
	for i, route := range ft.routes {
		fmt.Println("From: " + i)
		for _, filters := range route.filters {
			for _, filter := range filters {
				fmt.Println(filter)
			}
		}
	}
}

// SimpleAddSummarizedFilter is called upon receiving a subscription
// filter to see if it should be added if exclusive, merge
// with others or encompass or be encompassed by others
func (rs *RouteStats) SimpleAddSummarizedFilter(p *Predicate) (bool, *Predicate) {

	rs.routeLock.Lock()
	defer rs.routeLock.Unlock()

	merge := false

	for i, filters := range rs.filters {
		if len(p.attributes) > i {
			for _, f := range filters {
				if f.SimplePredicateMatch(p) {
					return true, nil
				}
			}
		} else if len(p.attributes) == i {
			for j := 0; j < len(filters); j++ {
				if filters[j].SimplePredicateMatch(p) {
					return true, nil
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
					merge = true
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
	if merge {
		return false, p
	}

	return false, nil
}

// SimpleSubtractFilter removes all the emcompassed filters by the
// info string predicate ignoring partial encompassing for now
func (rs *RouteStats) SimpleSubtractFilter(p *Predicate) {
	for i, filters := range rs.filters {
		if len(p.attributes) <= i {
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
}

func (rs *RouteStats) IsInterested(p *Predicate) bool {

	for i, filters := range rs.filters {
		if len(p.attributes) <= i {
			for _, filter := range filters {
				if filter.SimplePredicateMatch(p) {
					return true
				}
			}
		} else {
			break
		}
	}

	return false
}
