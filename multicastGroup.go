package contentpubsub

type MulticastGroup struct {
	predicate  *Predicate
	subByPlace map[string]map[string]*SubRegionData
	attrTrees  map[string]*RangeAttributeTree
}

// NewMulticastGroup
// Proto Version
func NewMulticastGroup(p *Predicate) *MulticastGroup {

	mg := &MulticastGroup{
		predicate:  p,
		subByPlace: make(map[string]map[string]*SubRegionData),
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

type SubRegionData struct {
	subs     []*SubData
	unhelped int
	helpers  []*SubData
}

// addSubToSubRegion
// Proto Version
func (sr *SubRegionData) addSubToSubRegion(sub *SubData) {

	sr.subs = append(sr.subs, sub)
	sr.unhelped++
}

// addSubToGroup
// Proto Version
func (mg *MulticastGroup) addSubToGroup(addr string, cap int, region string, subRegion string) {

	sub := &SubData{
		addr:      addr,
		capacity:  cap,
		region:    region,
		subRegion: subRegion,
	}

	mg.subByPlace[region][subRegion].addSubToSubRegion(sub)
}
