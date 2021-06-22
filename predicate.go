package contentpubsub

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type AttributeType int

const (
	Topic AttributeType = iota
	Range
)

// AttributeType can be Topic or Range
// Topic >> name: oil
// Range >> name: price, rangeQuery: [120,140]
type Attribute struct {
	name       string
	attrType   AttributeType
	rangeQuery [2]int
}

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
// an event or subscription, composed of one
// or more attributes
type Predicate struct {
	attributes map[string]*Attribute
}

// ToString returns the input representation of a predicate
func (p *Predicate) ToString() string {

	res := ""
	for _, attr := range p.attributes {
		res += attr.name

		if attr.attrType == Topic {
			res += " T/"
		} else {
			res += fmt.Sprintf(" R %d %d/", attr.rangeQuery[0], attr.rangeQuery[1])
		}
	}

	return res[:len(res)-1]
}

func (p *Predicate) String() string {
	res := ""

	for _, attr := range p.attributes {
		res += "<" + attr.String() + "> "
	}

	return res
}

// NewPredicate creates a predicate. Example of a rawPredicate:
// "laptop T/RAM R 16 32/price R 0 1000"
func NewPredicate(rawPredicate string, maxAttr int) (*Predicate, error) {

	attrs := make(map[string]*Attribute)
	firstParse := strings.Split(rawPredicate, "/")
	var limit int
	if len(firstParse) > maxAttr {
		limit = maxAttr
	} else {
		limit = len(firstParse)
	}

	for i := 0; i < limit; i++ {
		secondParse := strings.Split(firstParse[i], " ")
		if len(secondParse) < 2 {
			return nil, errors.New("invalid predicate input")
		}

		attr := &Attribute{name: secondParse[0]}

		if secondParse[1] == "T" {
			attr.attrType = Topic
		} else if secondParse[1] == "R" {
			attr.attrType = Range
			if len(secondParse) != 4 {
				return nil, errors.New("invalid range attribute input >> " + attr.name)
			}

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
// or also to know if a predicate encompasses other
// Special Note >> events range is seen as a single value, this means that
// a event will have equal values of the range attribute (Ex:"price R 15 15")
func (p *Predicate) SimplePredicateMatch(pEvent *Predicate) bool {

	for _, attr := range p.attributes {
		if _, ok := pEvent.attributes[attr.name]; !ok {
			return false
		} else if attr.attrType == Range {
			if !(pEvent.attributes[attr.name].rangeQuery[0] >= attr.rangeQuery[0] &&
				pEvent.attributes[attr.name].rangeQuery[1] <= attr.rangeQuery[1]) {
				return false
			}
		}
	}

	return true
}

// SimpleAdvMatch is used to check if a Premium Publisher advertisement
// is of the interest of a Premium Subscriber
func (p *Predicate) SimpleAdvMatch(pAdv *Predicate) bool {

	for _, attr := range p.attributes {
		if _, ok := pAdv.attributes[attr.name]; !ok {
			return false
		} else if attr.attrType == Range {
			if !(pAdv.attributes[attr.name].rangeQuery[0] <= attr.rangeQuery[0] &&
				pAdv.attributes[attr.name].rangeQuery[1] >= attr.rangeQuery[1]) {
				return false
			}
		}
	}

	return true
}

// TryMergePredicates is used in FilterSummarizing to attempt merging two
// different predicates. If the result is false it means they are exclusive,
// otherwise it will return the merge of both predicates
func (p *Predicate) TryMergePredicates(pOther *Predicate) (bool, *Predicate) {

	for _, attr := range p.attributes {
		if _, ok := pOther.attributes[attr.name]; !ok {
			return false, nil
		} else if attr.attrType == Range {

			if pOther.attributes[attr.name].rangeQuery[0] <= attr.rangeQuery[1] &&
				pOther.attributes[attr.name].rangeQuery[0] >= attr.rangeQuery[0] {
				pNew := pOther
				pNew.attributes[attr.name].rangeQuery[0] = attr.rangeQuery[0]

				return true, pNew
			} else if pOther.attributes[attr.name].rangeQuery[1] <= attr.rangeQuery[1] &&
				pOther.attributes[attr.name].rangeQuery[1] >= attr.rangeQuery[0] {
				pNew := pOther
				pNew.attributes[attr.name].rangeQuery[1] = attr.rangeQuery[1]

				return true, pNew
			}
		}
	}

	return false, nil
}

func (p *Predicate) Equal(pred *Predicate) bool {
	for _, attr := range p.attributes {
		if _, ok := pred.attributes[attr.name]; !ok {
			return false
		} else if attr.attrType == Range && pred.attributes[attr.name].attrType == Range &&
			attr.rangeQuery[0] != pred.attributes[attr.name].rangeQuery[0] &&
			attr.rangeQuery[1] != pred.attributes[attr.name].rangeQuery[1] {
			return false
		}

	}

	return true
}
