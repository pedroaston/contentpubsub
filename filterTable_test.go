package contentpubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Initializes a RouteStats for simple testing
func InitializeRouteStats() *RouteStats {
	r := NewRouteStats("10.10.10.10:1234")

	p1, err1 := NewPredicate("portugal T", 5)
	p2, err2 := NewPredicate("soccer T/goals R 2 5", 5)
	p3, err3 := NewPredicate("surf T/bali T/price R 500 850", 5)

	if err1 != nil || err2 != nil || err3 != nil {
		return nil
	}

	r.filters[1] = append(r.filters[1], p1)
	r.filters[2] = append(r.filters[2], p2)
	r.filters[3] = append(r.filters[3], p3)

	return r
}

// PrintRouteStats can be used for debug
func (r *RouteStats) PrintRouteStats() {

	fmt.Println("Printing RouteStat")

	for _, fs := range r.filters {
		for _, f := range fs {
			fmt.Println(f.String())
		}
	}

	fmt.Println("End Print!")
}

// Testing filter summarization in a RouteStat
func TestAddSummarizedFilter(t *testing.T) {

	r := InitializeRouteStats()
	// Test1: Bigger Predicate encompassed
	pTest, err := NewPredicate("portugal T/soccer T", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test1: Unexpected error creating Predicate")
	} else if len(r.filters[2]) == 2 {
		t.Fatal("Test1: Wrongly added filter: " + pTest.String())
	}

	// Test2: Same-size Predicate encompassed
	pTest, err = NewPredicate("soccer T/goals R 2 4", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test2: Unexpected error creating Predicate")
	} else if len(r.filters[2]) != 1 {
		t.Fatal("Test2: Wrongly added filter: " + pTest.String())
	}

	// Test3: Same-size Predicate encompassing
	pTest, err = NewPredicate("soccer T/goals R 2 7", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test3: Unexpected error creating Predicate")
	} else if len(r.filters[2]) == 2 {
		t.Fatal("Test3: Filter not deleted")
	} else if r.filters[2][0].attributes["goals"].rangeQuery[1] != 7 {
		t.Fatal("Test3: Wrong filter:" + r.filters[2][0].String())
	}

	// Test4: Smaller Predicate encompassing
	pTest, err = NewPredicate("soccer T", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test4: Unexpected error creating Predicate")
	} else if len(r.filters[1]) == 1 {
		t.Fatal("Test4: Filter not added: " + pTest.String())
	} else if len(r.filters[2]) != 0 {
		t.Fatal("Test4: Filter not deleted")
	}

	// Test5: Upper-limit merge
	pTest, err = NewPredicate("surf T/bali T/price R 700 1000", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test5: Unexpected error creating Predicate")
	} else if len(r.filters[3]) != 1 {
		t.Fatal("Test5: Filter not deleted")
	} else if r.filters[3][0].attributes["price"].rangeQuery[1] != 1000 {
		t.Fatal("Test5: Wrong merge:" + r.filters[3][0].String())
	}

	// Test6: Lower limit merge
	pTest, err = NewPredicate("surf T/bali T/price R 400 600", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test6: Unexpected error creating Predicate")
	} else if len(r.filters[3]) == 2 {
		t.Fatal("Test6: Filter not deleted")
	} else if r.filters[3][0].attributes["price"].rangeQuery[0] != 400 {
		t.Fatal("Test6: Wrong merge:" + r.filters[3][0].String())
	}

	// Test7: Exclusive predicate
	pTest, err = NewPredicate("Tesla T/stock R 10000 15000", 5)
	r.SimpleAddSummarizedFilter(pTest)

	if err != nil {
		t.Fatal("Test7: Unexpected error creating Predicate")
	} else if len(r.filters[2]) != 1 {
		t.Fatal("Test7: Filter not added: " + pTest.String())
	}
}

func TestNewFilterTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	ft := NewFilterTable(dhts[0])

	if len(ft.routes) != 4 {
		t.Fatal("Error creating filterTable")
	}
}
