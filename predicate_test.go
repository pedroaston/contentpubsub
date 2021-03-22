package contentpubsub

import (
	"testing"
)

// TestPredicateToAndFromString aimed to assure the correct
// functioning of NewPredicate and ToString functions
func TestPredicateToAndFromString(t *testing.T) {

	strPred := "goals R 2 5/portugal T/soccer T"
	p, err := NewPredicate(strPred)
	if err != nil {
		t.Fatal("Unexpected error forming a Predicate")
	}

	strTest := p.ToString()
	if strPred != strTest {
		t.Fatal("Invalid Transformation: " + strPred + " to " + strTest)
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
