package contentpubsub

import (
	"strconv"
)

type AttributeType int

const (
	Equality AttributeType = iota
	Quality
	Range
)

//Attribute is the minimal unity of a Predicate
type Attribute struct {
	name       string
	attrType   AttributeType
	extraInfo  string
	rangeQuery [2]int
}

// String returns the Attribute associated with a
func (a *Attribute) String() string {
	switch a.attrType {
	case Equality:
		return a.name
	case Quality:
		return a.name + " " + a.extraInfo
	case Range:
		return a.name + "from" + strconv.Itoa(a.rangeQuery[0]) + "to" + strconv.Itoa(a.rangeQuery[1])
	}

	return "nothing"
}

type Predicate struct {
	attributes map[string]*Attribute
}

func (p *Predicate) String() string {
	res := ""

	for _, attr := range p.attributes {
		res += attr.String()
	}

	return res
}
