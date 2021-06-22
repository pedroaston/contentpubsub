package contentpubsub

type Node struct {
	subs       []*SubData
	upperLimit int
	lowerLimit int
	left       *Node
	right      *Node
}

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

// InsertSub inserts a sub to all the nodes that he should
// be at, by recursively place it throught the tree
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

// GetSubsOfEvent returns all subs interested in a certain
// specific value by calling recursivelly the function
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

// DeleteSubsFromNode recursively called to delete a node throught a tree
func (n *Node) DeleteSubFromNode(upper int, lower int, sub *SubData) {

	localCap := n.upperLimit - n.lowerLimit + 1
	if upper >= n.upperLimit && lower <= n.lowerLimit {
		for i := 0; i < len(n.subs); i++ {
			if n.subs[i].addr == sub.addr {
				if i == 0 && len(n.subs) == 1 {
					n.subs = nil
				} else if i == 0 {
					n.subs = n.subs[1:]
				} else if len(n.subs) == i+1 {
					n.subs = n.subs[:i-1]
				} else {
					n.subs = append(n.subs[:i-1], n.subs[i+1:]...)
					i--
				}
			}
		}
	} else {
		if lower <= n.lowerLimit+localCap/2-1 {
			n.left.DeleteSubFromNode(upper, lower, sub)
		}
		if upper >= n.lowerLimit+localCap/2 {
			n.right.DeleteSubFromNode(upper, lower, sub)
		}
	}
}

// RangeAttributeTree is a structure that organizes the subscribers
// by their interest in a specific value of an attribute for
// then faster/efficient subscriber diffusion
type RangeAttributeTree struct {
	root       *Node
	attrname   string
	upperValue int
	lowerValue int
}

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

// AddSubToTreeRoot adds a sub to a tree root
func (rt *RangeAttributeTree) AddSubToTreeRoot(sub *SubData) {
	rt.root.subs = append(rt.root.subs, sub)
}

// RemoveSubFromTreeRoot removes a sub from the tree root
func (rt *RangeAttributeTree) RemoveSubFromTreeRoot(sub *SubData) {

	for i := 0; i < len(rt.root.subs); i++ {
		if rt.root.subs[i].addr == sub.addr {
			if i == 0 && len(rt.root.subs) == 1 {
				rt.root.subs = nil
			} else if i == 0 {
				rt.root.subs = rt.root.subs[1:]
			} else if len(rt.root.subs) == i+1 {
				rt.root.subs = rt.root.subs[:i-1]
			} else {
				rt.root.subs = append(rt.root.subs[:i-1], rt.root.subs[i+1:]...)
				i--
			}
		}
	}
}

// AddSubToTree adds a sub to the tree translating
// the attribute values for tree insertion
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

// GetInterestedSubs collects the subs that are
// interested in an attribute specific value
func (rt *RangeAttributeTree) GetInterestedSubs(value int) []*SubData {

	return rt.root.GetSubsOfEvent(value - rt.lowerValue)
}

// DeleteSubFromTree removes a subs from the tree
func (rt *RangeAttributeTree) DeleteSubFromTree(sub *SubData) {

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

	rt.root.DeleteSubFromNode(upper, lower, sub)
}
