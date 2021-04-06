package contentpubsub

import "sort"

const (
	MaxSubsPerRegion  = 5
	PowerSubsPoolSize = 2
)

type MulticastGroup struct {
	predicate  *Predicate
	subByPlace map[string]map[string]*SubRegionData
	trackHelp  map[string]*HelperTracker
	attrTrees  map[string]*RangeAttributeTree
}

// NewMulticastGroup
// Proto Version
func NewMulticastGroup(p *Predicate) *MulticastGroup {

	mg := &MulticastGroup{
		predicate:  p,
		subByPlace: make(map[string]map[string]*SubRegionData),
		trackHelp:  make(map[string]*HelperTracker),
	}

	for _, attr := range p.attributes {
		if attr.attrType == Range {
			mg.attrTrees[attr.name] = &RangeAttributeTree{}
		}
	}

	return mg
}

type SubData struct {
	pred      *Predicate
	addr      string
	capacity  int
	region    string
	subRegion string
}

type ByCapacity []*SubData

func (a ByCapacity) Len() int           { return len(a) }
func (a ByCapacity) Less(i, j int) bool { return a[i].capacity < a[j].capacity }
func (a ByCapacity) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type SubRegionData struct {
	subs     []*SubData
	unhelped int
	helpers  []*SubData
}

type HelperTracker struct {
	helper        *SubData
	subsDelegated []*SubData
	remainCap     int
}

// addSubToSubRegion
// Proto Version
func (sr *SubRegionData) addSubToSubRegion(sub *SubData) {

	sr.subs = append(sr.subs, sub)
	sr.unhelped++
}

// addSubToGroup
// Proto Version
func (mg *MulticastGroup) addSubToGroup(addr string, cap int, region string, subRegion string, pred *Predicate) {

	sub := &SubData{
		pred:      pred,
		addr:      addr,
		capacity:  cap,
		region:    region,
		subRegion: subRegion,
	}

	subReg := mg.subByPlace[region][subRegion]

	var lastHelper *SubData = nil
	if len(subReg.helpers) != 0 {
		lastHelper = subReg.helpers[len(subReg.helpers)-1]

		if mg.trackHelp[lastHelper.addr].remainCap > 0 {
			mg.AddSubToHelper(sub, lastHelper.addr)
		}
	} else {
		if subReg.unhelped+1 > MaxSubsPerRegion {
			candidate := subReg.subs[0]
			var indexCutter int
			if candidate.capacity > subReg.unhelped-PowerSubsPoolSize+1 {
				indexCutter = PowerSubsPoolSize
			} else {
				indexCutter = subReg.unhelped - candidate.capacity + 1
			}

			// TODO >> Starts remote process that needs confirmation
			RecruitHelper(candidate, append(subReg.subs[indexCutter:], sub))
			subReg.helpers = append(subReg.helpers, candidate)
			toDelegate := append(subReg.subs[indexCutter+1:], sub)
			subReg.subs = subReg.subs[1:indexCutter]
			subReg.unhelped = len(subReg.subs)
			mg.trackHelp[candidate.addr] = &HelperTracker{
				helper:        candidate,
				subsDelegated: toDelegate,
				remainCap:     candidate.capacity - len(toDelegate),
			}

			for _, sub := range append(toDelegate, candidate) {
				mg.RemoveFromRangeTrees(sub)
			}

		} else {
			subReg.subs = append(subReg.subs, sub)
			sort.Sort(ByCapacity(subReg.subs))
			subReg.unhelped++
			mg.AddToRangeTrees(sub)
		}
	}

	for _, attr := range sub.pred.attributes {
		if attr.attrType == Range {
			mg.attrTrees[attr.name].AddSubToTree(sub)
		}
	}
}

// RecruitHelper
// INCOMPLETE
func RecruitHelper(helper *SubData, subs []*SubData) error {

	return nil
}

// AddSubToHelper
// INCOMPLETE
func (mg *MulticastGroup) AddSubToHelper(sub *SubData, addr string) {

}

// AddToRangeTrees
func (mg *MulticastGroup) AddToRangeTrees(sub *SubData) {
	for attr, tree := range mg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.AddSubToTreeRoot(sub)
		} else {
			tree.AddSubToTree(sub)
		}
	}
}

// RemoveFromRangeTrees
func (mg *MulticastGroup) RemoveFromRangeTrees(sub *SubData) {
	for attr, tree := range mg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.RemoveSubFromTreeRoot(sub)
		} else {
			tree.DeleteSubFromTree(sub)
		}
	}
}

// PublishEvent
// INCOMPLETE
func (mg *MulticastGroup) PublishEvent(p *Predicate, data string) {
	var interested []*SubData = nil
	for attr, tree := range mg.attrTrees {
		if interested == nil {
			interested = tree.GetInterestedSubs(p.attributes[attr].rangeQuery[0])
		} else {
			plus := tree.GetInterestedSubs(p.attributes[attr].rangeQuery[0])
			aux := interested
			interested = nil
			for _, sub1 := range aux {
				for _, sub2 := range plus {
					if sub1.addr == sub2.addr {
						interested = append(interested, sub1)
					}
				}
			}
		}
	}

	// TODO >> Send to all subs in interested

}
