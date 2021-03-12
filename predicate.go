package contentpubsub

import (
	"strconv"
	"strings"
)

// AttributeType can be Topic or Range
type AttributeType int

// Topic >> name: portugal
// Range >> name: price, rangeQuery: [120,140]
const (
	Topic AttributeType = iota
	Range
)

// Attribute is the minimal unit of a Predicate and can be:
type Attribute struct {
	name       string
	attrType   AttributeType
	rangeQuery [2]int
}

// String returns representation of an Attribute
func (a *Attribute) String() string {

	switch a.attrType {
	case Topic:
		return a.name
	case Range:
		return a.name + " from " + strconv.Itoa(a.rangeQuery[0]) + " to " + strconv.Itoa(a.rangeQuery[1])
	default:
		return ""
	}
}

// Predicate is expression that categorizes
// an event or subscription
type Predicate struct {
	attributes map[string]*Attribute
}

func (p *Predicate) String() string {
	res := ""

	for _, attr := range p.attributes {
		res += "<" + attr.String() + "> "
	}

	return res
}

// NewPredicate creates a predicate. Example of info string:
// laptop T/RAM R 16 32/price R 0 1000
func NewPredicate(info string) (*Predicate, error) {

	attrs := make(map[string]*Attribute)
	firstParse := strings.Split(info, "/")

	for _, value := range firstParse {
		secondParse := strings.Split(value, " ")
		attr := &Attribute{name: secondParse[0]}

		if secondParse[1] == "T" {
			attr.attrType = Topic
		} else if secondParse[1] == "R" {
			attr.attrType = Range

			val0, err1 := strconv.Atoi(secondParse[2])
			val1, err2 := strconv.Atoi(secondParse[3])

			if err1 != nil {
				return nil, err1
			} else if err2 != nil {
				return nil, err2
			}

			attr.rangeQuery[0] = val0
			attr.rangeQuery[1] = val1
		}

		attrs[attr.name] = attr
	}

	p := &Predicate{attributes: attrs}

	return p, nil
}

// SimplePredicateMatch evaluates if an event predicate matches a sub predicate
// Special Note >> events range is seen as a single value for now for simplicity
// and lack of reason to support a range
func (p *Predicate) simplePredicateMatch(pEvent *Predicate) bool {

	for _, attr := range p.attributes {
		if _, ok := pEvent.attributes[attr.name]; !ok {
			return false
		} else if attr.attrType == Range {
			if pEvent.attributes[attr.name].rangeQuery[0] >= attr.rangeQuery[0] &&
				pEvent.attributes[attr.name].rangeQuery[1] <= attr.rangeQuery[1] {
				return false
			}
		}
	}

	return true
}
