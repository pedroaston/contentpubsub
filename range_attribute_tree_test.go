package contentpubsub

import (
	"testing"
)

// TestAddSubsToTreeAndRead tests if the adding function works properly
// as also the process to return the interested subs of a event
func TestAddSubsToTreeAndRead(t *testing.T) {

	attr := &Attribute{
		name:     "year",
		attrType: Range,
	}
	attr.rangeQuery[0] = 2010
	attr.rangeQuery[1] = 2017

	rt := NewRangeAttributeTree(attr)

	pred1, _ := NewPredicate("wine T/year R 2011 2011", 5)
	sub1 := &SubData{
		pred: pred1,
		addr: "just for testing 1",
	}

	rt.AddSubToTree(sub1)

	pred2, _ := NewPredicate("wine T/year R 2010 2013", 5)
	sub2 := &SubData{
		pred: pred2,
		addr: "just for testing 2",
	}

	rt.AddSubToTree(sub2)

	pred3, _ := NewPredicate("wine T/year R 2010 2015", 5)
	sub3 := &SubData{
		pred: pred3,
		addr: "just for testing 3",
	}

	rt.AddSubToTree(sub3)

	pred4, _ := NewPredicate("wine T/year R 2010 2017", 5)
	sub4 := &SubData{
		pred: pred4,
		addr: "just for testing 4",
	}

	rt.AddSubToTree(sub4)

	if len(rt.root.subs) != 1 {
		t.Fatal("Error 1")
	}
	if len(rt.root.right.left.subs) != 1 {
		t.Fatal("Error 2")
	}
	if len(rt.root.left.subs) != 2 {
		t.Fatal("Error 3")
	}
	if len(rt.root.left.left.right.subs) != 1 {
		t.Fatal("Error 4")
	}
	if len(rt.GetInterestedSubs(2011)) != 4 {
		t.Fatal("Wrong number of interested subs")
	}
}
