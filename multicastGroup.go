package contentpubsub

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
	"google.golang.org/grpc"
)

const (
	MaxSubsPerRegion  = 5
	PowerSubsPoolSize = 2
)

type MulticastGroup struct {
	selfAddr   string
	predicate  *Predicate
	subByPlace map[string]map[string]*SubRegionData
	trackHelp  map[string]*HelperTracker
	attrTrees  map[string]*RangeAttributeTree
}

// NewMulticastGroup
// Proto Version
func NewMulticastGroup(p *Predicate, addr string) *MulticastGroup {

	mg := &MulticastGroup{
		selfAddr:   addr,
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

// addSubToGroup
// Proto Version
func (mg *MulticastGroup) addSubToGroup(addr string, cap int, region string, subRegion string, pred *Predicate) error {

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

			err := mg.RecruitHelper(candidate, append(subReg.subs[indexCutter:], sub))
			if err != nil {
				return err
			}

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

	return nil
}

// RecruitHelper
func (mg *MulticastGroup) RecruitHelper(helper *SubData, subs []*SubData) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pubAddr := helper.addr
	conn, err := grpc.Dial(pubAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	gID := &pb.MulticastGroupID{
		OwnerAddr: mg.selfAddr,
		Predicate: mg.predicate.ToString(),
	}

	var aux map[int32]*pb.MinimalSubData = make(map[int32]*pb.MinimalSubData)
	for i, sub := range subs {
		mSub := &pb.MinimalSubData{
			Addr:      sub.addr,
			Predicate: sub.pred.ToString(),
		}

		aux[int32(i)] = mSub
	}

	req := &pb.HelpRequest{
		GroupID: gID,
		Subs:    aux,
	}

	client := pb.NewScoutHubClient(conn)
	ack, err := client.RequestHelp(ctx, req)
	if !ack.State && err != nil {
		return err
	}

	return nil
}

// AddSubToHelper
// INCOMPLETE
func (mg *MulticastGroup) AddSubToHelper(sub *SubData, addr string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	gID := &pb.MulticastGroupID{
		OwnerAddr: mg.selfAddr,
		Predicate: mg.predicate.ToString(),
	}

	protoSub := &pb.MinimalSubData{
		Addr:      sub.addr,
		Predicate: sub.pred.ToString(),
	}

	help := &pb.DelegateSub{
		GroupID: gID,
		Sub:     protoSub,
	}

	client := pb.NewScoutHubClient(conn)
	ack, err := client.DelegateSubToHelper(ctx, help)
	if !ack.State && err != nil {
		return err
	}

	return nil
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

	// TODO >> Send to all subs in interested plus helpers

}

type SubGroupView struct {
	pubAddr   string
	predicate *Predicate
	helping   bool
	attrTrees map[string]*RangeAttributeTree
}

// SetHasHelper
func (sg *SubGroupView) SetHasHelper(req *pb.HelpRequest) error {

	sg.helping = true
	for _, attr := range sg.predicate.attributes {
		if attr.attrType == Range {
			sg.attrTrees[attr.name] = &RangeAttributeTree{}
		}
	}

	for _, sub := range req.Subs {
		sg.AddSub(sub)
	}

	return nil
}

// AddSub
func (sg *SubGroupView) AddSub(sub *pb.MinimalSubData) error {

	p, err := NewPredicate(sub.Predicate)
	if err != nil {
		return err
	}

	protoSub := &SubData{
		addr: sub.Addr,
		pred: p,
	}
	sg.AddToRangeTrees(protoSub)

	return nil
}

// AddToRangeTrees
func (sg *SubGroupView) AddToRangeTrees(sub *SubData) {
	for attr, tree := range sg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.AddSubToTreeRoot(sub)
		} else {
			tree.AddSubToTree(sub)
		}
	}
}
