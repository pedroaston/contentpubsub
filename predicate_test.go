package contentpubsub

import (
	"testing"
)

func TestPredicateMaxSize(t *testing.T) {

	p, err := NewPredicate("soccer T/portugal T/slb T/stadium T/eusebio T/red T")
	if err != nil {
		t.Fatal(err)
	}

	if len(p.attributes) != 5 {
		t.Fatal()
	}
}

// Testing event predicate matching with Sub predicate
// by analyzing all border scenarios
func TestPredicateMatching(t *testing.T) {

	var res bool
	// Lower Range bound >> Match
	pEvent, err1 := NewPredicate("soccer T/portugal T/goals R 3 3")
	pSub, err2 := NewPredicate("soccer T/goals R 3 10")

	if err1 == nil && err2 == nil {
		res = pSub.SimplePredicateMatch(pEvent)
	} else {
		t.Fatal("Not supposed error creating the predicates")
	}

	if !res {
		t.Fatal("Wrong Matching between: " + pSub.String() +
			" and " + pEvent.String())
	}

	// Upper Range bound >> Match
	pEvent, err1 = NewPredicate("soccer T/portugal T/goals R 10 10")

	if err1 == nil && err2 == nil {
		res = pSub.SimplePredicateMatch(pEvent)
	} else {
		t.Fatal("Not supposed error creating the predicates")
	}

	if !res {
		t.Fatal("Wrong Matching between: " + pSub.String() +
			" and " + pEvent.String())
	}

	// Inside Range >> Match
	pEvent, err1 = NewPredicate("soccer T/portugal T/goals R 5 5")

	if err1 == nil && err2 == nil {
		res = pSub.SimplePredicateMatch(pEvent)
	} else {
		t.Fatal("Not supposed error creating the predicates")
	}

	if !res {
		t.Fatal("Wrong Matching between: " + pSub.String() +
			" and " + pEvent.String())
	}

	// Lower Range bound >> missMatch
	pEvent, err1 = NewPredicate("soccer T/portugal T/goals R 2 2")

	if err1 == nil && err2 == nil {
		res = pSub.SimplePredicateMatch(pEvent)
	} else {
		t.Fatal("Not supposed error creating the predicates")
	}

	if res {
		t.Fatal("Wrong Matching between: " + pSub.String() +
			" and " + pEvent.String())
	}

	// Upper Range bound >> missMatch
	pEvent, err1 = NewPredicate("soccer T/portugal T/goals R 11 11")

	if err1 == nil && err2 == nil {
		res = pSub.SimplePredicateMatch(pEvent)
	} else {
		t.Fatal("Not supposed error creating the predicates")
	}

	if res {
		t.Fatal("Wrong Matching between: " + pSub.String() +
			" and " + pEvent.String())
	}
}

// TestSubEventMatching
func TestSubEventMatching(t *testing.T) {

	pEvent, _ := NewPredicate("portugal T/trip T/surf T/price R 1200 1200")

	pSub1, _ := NewPredicate("portugal T")
	pSub2, _ := NewPredicate("portugal T/trip T")
	pSub3, _ := NewPredicate("portugal T/trip T/surf T")
	pSub4, _ := NewPredicate("portugal T/trip T/surf T/price R 1000 1500")
	pSub5, _ := NewPredicate("trip T/surf T/price R 1000 1500")

	if !pSub1.SimplePredicateMatch(pEvent) {
		t.Fatal("error matching at 1")
	}

	if !pSub2.SimplePredicateMatch(pEvent) {
		t.Fatal("error matching at 2")
	}

	if !pSub3.SimplePredicateMatch(pEvent) {
		t.Fatal("error matching at 3")
	}

	if !pSub4.SimplePredicateMatch(pEvent) {
		t.Fatal("error matching at 4")
	}

	if !pSub5.SimplePredicateMatch(pEvent) {
		t.Fatal("error matching at 5")
	}
}
