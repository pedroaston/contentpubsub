package contentpubsub

import (
	"testing"
)

//
func TestPredicateMaxSize(t *testing.T) {

	p, err := NewPredicate("soccer T/portugal T/slb T/stadium T/eusebio T/red T", 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(p.attributes) != 5 {
		t.Fatal()
	}
}

// Testi event predicate matching with Sub predicate
// by analyzing all border scenarios
func TestPredicateMatching(t *testing.T) {

	pSub, _ := NewPredicate("soccer T/goals R 3 10", 5)

	// Lower Range bound >> Match
	pEvent, _ := NewPredicate("soccer T/portugal T/goals R 3 3", 5)

	if !pSub.SimplePredicateMatch(pEvent) {
		t.Fatal("Wrong Matching 1")
	}

	// Upper Range bound >> Match
	pEvent, _ = NewPredicate("soccer T/portugal T/goals R 10 10", 5)

	if !pSub.SimplePredicateMatch(pEvent) {
		t.Fatal("Wrong Matching 2")
	}

	// Inside Range >> Match
	pEvent, _ = NewPredicate("soccer T/portugal T/goals R 5 5", 5)

	if !pSub.SimplePredicateMatch(pEvent) {
		t.Fatal("Wrong Matching 3")
	}

	// Lower Range bound >> missMatch
	pEvent, _ = NewPredicate("soccer T/portugal T/goals R 2 2", 5)

	if pSub.SimplePredicateMatch(pEvent) {
		t.Fatal("Wrong Matching 4")
	}

	// Upper Range bound >> missMatch
	pEvent, _ = NewPredicate("soccer T/portugal T/goals R 11 11", 5)

	if pSub.SimplePredicateMatch(pEvent) {
		t.Fatal("Wrong Matching 4")
	}

}
