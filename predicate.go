package contentpubsub

import (
	"strconv"
	"strings"
)

type AttributeType int

const (
	Topic AttributeType = iota
	Range
)

// Attribute is the minimal unity of a Predicate and can be:
// Topic >> name: portugal
// Range >> name: price, rangeQuery: [120,140]
type Attribute struct {
	name       string
	attrType   AttributeType
	rangeQuery [2]int
}

// String returns representation of a
func (a *Attribute) String() string {

	switch a.attrType {
	case Topic:
		return a.name
	case Range:
		return a.name + "from" + strconv.Itoa(a.rangeQuery[0]) + "to" + strconv.Itoa(a.rangeQuery[1])
	default:
		return ""
	}
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

// Function that creates a predicate. Example of info string:
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

// TODO >> still to be done
// Simple method to see if two predicates match
func (p *Predicate) simplePredicateMatch(pMatch *Predicate) bool {

	return true
}
