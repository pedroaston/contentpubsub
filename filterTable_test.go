package contentpubsub

import (
	"context"
	"testing"
	"time"
)

func InitializeRouteStats() *RouteStats {
	r := NewRouteStats("indifferent")

	p1, _ := NewPredicate("portugal T", 5)
	p2, _ := NewPredicate("soccer T/goals R 2 5", 5)
	p3, _ := NewPredicate("surf T/bali T/price R 500 850", 5)

	r.filters[1] = append(r.filters[1], p1)
	r.filters[2] = append(r.filters[2], p2)
	r.filters[3] = append(r.filters[3], p3)

	return r
}

// TestAddSummarizedFilter validates filter
// summarization in a RouteStat
func TestAddSummarizedFilter(t *testing.T) {

	r := InitializeRouteStats()
	// Test1: Bigger Predicate encompassed
	pTest, _ := NewPredicate("portugal T/soccer T", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[2]) == 2 {
		t.Fatal("Test1: Wrongly added filter: " + pTest.String())
	}

	// Test2: Same-size Predicate encompassed
	pTest, _ = NewPredicate("soccer T/goals R 2 4", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[2]) != 1 {
		t.Fatal("Test2: Wrongly added filter: " + pTest.String())
	}

	// Test3: Same-size Predicate encompassing
	pTest, _ = NewPredicate("soccer T/goals R 2 7", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[2]) == 2 {
		t.Fatal("Test3: Filter not deleted")
	} else if r.filters[2][0].attributes["goals"].rangeQuery[1] != 7 {
		t.Fatal("Test3: Wrong filter:" + r.filters[2][0].String())
	}

	// Test4: Smaller Predicate encompassing
	pTest, _ = NewPredicate("soccer T", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[1]) == 1 {
		t.Fatal("Test4: Filter not added: " + pTest.String())
	} else if len(r.filters[2]) != 0 {
		t.Fatal("Test4: Filter not deleted")
	}

	// Test5: Upper-limit merge
	pTest, _ = NewPredicate("surf T/bali T/price R 700 1000", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[3]) != 1 {
		t.Fatal("Test5: Filter not deleted")
	} else if r.filters[3][0].attributes["price"].rangeQuery[1] != 1000 {
		t.Fatal("Test5: Wrong merge:" + r.filters[3][0].String())
	}

	// Test6: Lower limit merge
	pTest, _ = NewPredicate("surf T/bali T/price R 400 600", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[3]) == 2 {
		t.Fatal("Test6: Filter not deleted")
	} else if r.filters[3][0].attributes["price"].rangeQuery[0] != 400 {
		t.Fatal("Test6: Wrong merge:" + r.filters[3][0].String())
	}

	// Test7: Exclusive predicate
	pTest, _ = NewPredicate("Tesla T/stock R 10000 15000", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if len(r.filters[2]) != 1 {
		t.Fatal("Test7: Filter not added: " + pTest.String())
	}
}

// TestNewFilterTable validates the initialization of a filterTable
func TestNewFilterTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	ft := NewFilterTable(dhts[0], false)

	if len(ft.routes) != 4 {
		t.Fatal("Error creating filterTable")
	}
}
