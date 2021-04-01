package contentpubsub

type Node struct {
	subs       []*SubData
	upperLimit int
	lowerLimit int
	left       *Node
	right      *Node
}

// NewNode creates a node on the attributeTree with a certain range
func NewNode(upperLimit int, lowerLimit int) *Node {

	n := &Node{
		upperLimit: upperLimit,
		lowerLimit: lowerLimit,
	}

	if n.lowerLimit == n.upperLimit {
		return n
	}

	localCap := n.upperLimit - n.lowerLimit
	n.left = NewNode(n.lowerLimit+localCap/2, n.lowerLimit)
	n.right = NewNode(n.upperLimit, n.lowerLimit+localCap/2+1)

	return n
}

// InsertSub inserts a sub to all the nodes that he should be
// recursively throught the tree
func (n *Node) InsertSub(upper int, lower int, sub *SubData) {

	localCap := n.upperLimit - n.lowerLimit + 1
	if upper >= n.upperLimit && lower <= n.lowerLimit {
		n.subs = append(n.subs, sub)
	} else {
		if lower <= n.lowerLimit+localCap/2-1 {
			n.left.InsertSub(upper, lower, sub)
		}
		if upper >= n.lowerLimit+localCap/2 {
			n.right.InsertSub(upper, lower, sub)
		}
	}
}

// GetSubsOfEvent returns all subs by calling recursivelly the function
func (n *Node) GetSubsOfEvent(value int) []*SubData {

	localCap := n.upperLimit - n.lowerLimit + 1
	if n.left == nil {
		return n.subs
	} else if value <= n.lowerLimit+localCap/2-1 {
		return append(n.subs, n.left.GetSubsOfEvent(value)...)
	} else {
		return append(n.subs, n.right.GetSubsOfEvent(value)...)
	}

}

type RangeAttributeTree struct {
	root       *Node
	attrname   string
	upperValue int
	lowerValue int
}

// NewRangeAttributeTree creates a range attribute tree with a certain range
func NewRangeAttributeTree(attr *Attribute) *RangeAttributeTree {

	size := attr.rangeQuery[1] - attr.rangeQuery[0] + 1
	cap := 2

	for {
		if cap >= size {
			break
		}

		cap = cap * 2
	}

	rt := &RangeAttributeTree{
		attrname:   attr.name,
		lowerValue: attr.rangeQuery[0],
		upperValue: attr.rangeQuery[1],
	}

	rt.root = NewNode(cap-1, 0)

	return rt
}

// AddSubToTree adds a sub to the tree translating the attribute values for tree insertion
func (rt *RangeAttributeTree) AddSubToTree(sub *SubData) {

	var upper, lower int

	if sub.pred.attributes[rt.attrname].rangeQuery[1] >= rt.upperValue {
		upper = rt.upperValue
	} else {
		upper = sub.pred.attributes[rt.attrname].rangeQuery[1] - rt.lowerValue
	}

	if sub.pred.attributes[rt.attrname].rangeQuery[0] <= rt.lowerValue {
		lower = 0
	} else {
		lower = sub.pred.attributes[rt.attrname].rangeQuery[0] - rt.lowerValue
	}

	rt.root.InsertSub(upper, lower, sub)
}

// GetInterestedSubs
func (rt *RangeAttributeTree) GetInterestedSubs(value int) []*SubData {

	return rt.root.GetSubsOfEvent(value - rt.lowerValue)
}
