package contentpubsub

import (
	"context"
	"errors"
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
	helpers    []*SubData
	attrTrees  map[string]*RangeAttributeTree
	simpleList []*SubData
}

// NewMulticastGroup
func NewMulticastGroup(p *Predicate, addr string) *MulticastGroup {

	mg := &MulticastGroup{
		selfAddr:   addr,
		predicate:  p,
		subByPlace: make(map[string]map[string]*SubRegionData),
		trackHelp:  make(map[string]*HelperTracker),
		attrTrees:  make(map[string]*RangeAttributeTree),
	}

	for _, attr := range p.attributes {
		if attr.attrType == Range {
			mg.attrTrees[attr.name] = NewRangeAttributeTree(attr)
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
func (a ByCapacity) Less(i, j int) bool { return a[i].capacity > a[j].capacity }
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
func (mg *MulticastGroup) addSubToGroup(addr string, cap int, region string, subRegion string, pred *Predicate) error {

	newSub := &SubData{
		pred:      pred,
		addr:      addr,
		capacity:  cap,
		region:    region,
		subRegion: subRegion,
	}

	if _, ok := mg.subByPlace[region]; !ok {
		subReg := &SubRegionData{unhelped: 0}
		mg.subByPlace[region] = map[string]*SubRegionData{}
		mg.subByPlace[region][subRegion] = subReg
	} else if _, ok := mg.subByPlace[region][subRegion]; !ok {
		subReg := &SubRegionData{unhelped: 0}
		mg.subByPlace[region][subRegion] = subReg
	}

	subReg := mg.subByPlace[region][subRegion]
	if len(subReg.helpers) != 0 && mg.trackHelp[subReg.helpers[len(subReg.helpers)-1].addr].remainCap > 0 {
		lastHelper := subReg.helpers[len(subReg.helpers)-1]
		mg.AddSubToHelper(newSub, lastHelper.addr)
	} else {
		if subReg.unhelped+1 > MaxSubsPerRegion {
			candidate := subReg.subs[0]
			var indexCutter int
			if candidate.capacity > subReg.unhelped-PowerSubsPoolSize+1 {
				indexCutter = PowerSubsPoolSize + 1
			} else {
				indexCutter = subReg.unhelped - candidate.capacity + 1
			}

			err := mg.RecruitHelper(candidate, append(subReg.subs[indexCutter:], newSub))
			if err != nil {
				return err
			}

			mg.trackHelp[candidate.addr] = &HelperTracker{
				helper:        candidate,
				subsDelegated: append(subReg.subs[indexCutter:], newSub),
				remainCap:     candidate.capacity - len(append(subReg.subs[indexCutter:], newSub)),
			}

			subReg.helpers = append(subReg.helpers, candidate)
			toDelegate := subReg.subs[indexCutter:]
			subReg.subs = subReg.subs[1:indexCutter]
			subReg.unhelped = len(subReg.subs)

			for _, sub := range toDelegate {
				if len(mg.attrTrees) == 0 {
					mg.RemoveSubFromList(sub)
				} else {
					mg.RemoveFromRangeTrees(sub)
				}
			}

			if len(mg.attrTrees) == 0 {
				mg.RemoveSubFromList(candidate)
			} else {
				mg.RemoveFromRangeTrees(candidate)
			}

		} else {
			subReg.subs = append(subReg.subs, newSub)
			sort.Sort(ByCapacity(subReg.subs))
			subReg.unhelped++
			if len(mg.attrTrees) == 0 {
				mg.simpleList = append(mg.simpleList, newSub)
			} else {
				mg.AddToRangeTrees(newSub)
			}
		}
	}

	return nil
}

// RemoveSubFromGroup
func (mg *MulticastGroup) RemoveSubFromGroup(sub *pb.PremiumSubscription) error {

	for i, s := range mg.subByPlace[sub.Region][sub.SubRegion].subs {
		if s.addr == sub.Addr {
			toRemove := s
			if i == 0 {
				mg.subByPlace[sub.Region][sub.SubRegion].subs = mg.subByPlace[sub.Region][sub.SubRegion].subs[1:]
			} else if i+1 == len(mg.subByPlace[sub.Region][sub.SubRegion].subs) {
				mg.subByPlace[sub.Region][sub.SubRegion].subs = mg.subByPlace[sub.Region][sub.SubRegion].subs[:i-1]
			} else {
				mg.subByPlace[sub.Region][sub.SubRegion].subs = append(mg.subByPlace[sub.Region][sub.SubRegion].subs[:i-1],
					mg.subByPlace[sub.Region][sub.SubRegion].subs[i+1:]...)
			}

			mg.subByPlace[sub.Region][sub.SubRegion].unhelped--
			if len(mg.attrTrees) == 0 {
				mg.RemoveSubFromList(toRemove)
			} else {
				mg.RemoveFromRangeTrees(toRemove)
			}

			return nil
		}
	}

	for _, helper := range mg.trackHelp {
		if helper.helper.addr == sub.Addr {
			helperFailedSubs := helper.subsDelegated
			mg.StopDelegating(helper, false)

			for _, sub := range helperFailedSubs {
				mg.addSubToGroup(sub.addr, sub.capacity, sub.region, sub.subRegion, sub.pred)
			}

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	for _, helper := range mg.subByPlace[sub.Region][sub.SubRegion].helpers {
		for i, s := range mg.trackHelp[helper.addr].subsDelegated {
			if s.addr == sub.Addr {
				conn, err := grpc.Dial(helper.addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("fail to dial: %v", err)
				}
				defer conn.Close()

				sub.OwnPredicate = s.pred.ToString()

				client := pb.NewScoutHubClient(conn)
				ack, err := client.PremiumUnsubscribe(ctx, sub)
				if !ack.State && err != nil {
					return errors.New("failed to unsubscribe from helper")
				}

				if i == 0 {
					mg.trackHelp[helper.addr].subsDelegated = mg.trackHelp[helper.addr].subsDelegated[1:]
				} else if i+1 == len(mg.subByPlace[sub.Region][sub.SubRegion].subs) {
					mg.trackHelp[helper.addr].subsDelegated = mg.trackHelp[helper.addr].subsDelegated[:i-1]
				} else {
					mg.trackHelp[helper.addr].subsDelegated = append(mg.trackHelp[helper.addr].subsDelegated[:i-1],
						mg.trackHelp[helper.addr].subsDelegated[i+1:]...)
				}

				mg.trackHelp[helper.addr].helper.capacity++
				return nil
			}
		}
	}

	return nil
}

// RecruitHelper
func (mg *MulticastGroup) RecruitHelper(helper *SubData, subs []*SubData) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	conn, err := grpc.Dial(helper.addr, grpc.WithInsecure())
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
	if err != nil || !ack.State {
		return err
	}

	mg.helpers = append(mg.helpers, helper)
	hT := &HelperTracker{
		helper:        helper,
		subsDelegated: subs,
		remainCap:     helper.capacity - len(subs),
	}
	mg.trackHelp[helper.addr] = hT

	return nil
}

// AddSubToHelper
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

	mg.trackHelp[addr].subsDelegated = append(mg.trackHelp[addr].subsDelegated, sub)
	mg.trackHelp[addr].remainCap--

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

// RemoveSubFromList
func (mg *MulticastGroup) RemoveSubFromList(sub *SubData) {

	for i, s := range mg.simpleList {
		if s.addr == sub.addr {
			if i == 0 {
				mg.simpleList = mg.simpleList[1:]
			} else if i+1 == len(mg.simpleList) {
				mg.simpleList = mg.simpleList[:i]
			} else {
				mg.simpleList = append(mg.simpleList[:i], mg.simpleList[i+1:]...)
			}
		}
	}
}

// AddrsToPublishEvent
func (mg *MulticastGroup) AddrsToPublishEvent(p *Predicate) []*SubData {

	var interested []*SubData = nil
	if len(mg.attrTrees) == 0 {
		return mg.simpleList
	} else {
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
	}

	return interested
}

// StopDelegating
func (mg *MulticastGroup) StopDelegating(tracker *HelperTracker, add bool) {

	for i := 0; i < len(mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers); i++ {
		if mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers[i].addr == tracker.helper.addr {
			if i == 0 {
				mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers = mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers[1:]
			} else if len(mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers) == i+1 {
				mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers = mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers[:i-1]
			} else {
				mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers = append(mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers[:i-1],
					mg.subByPlace[tracker.helper.region][tracker.helper.subRegion].helpers[i+1:]...)
			}

			break
		}
	}

	delete(mg.trackHelp, tracker.helper.addr)

	if add {
		tracker.helper.capacity = 0
	}
}

type SubGroupView struct {
	pubAddr    string
	predicate  *Predicate
	helping    bool
	attrTrees  map[string]*RangeAttributeTree
	simpleList []*SubData
}

// SetHasHelper
func (sg *SubGroupView) SetHasHelper(req *pb.HelpRequest) error {

	sg.helping = true
	for _, attr := range sg.predicate.attributes {
		if attr.attrType == Range {
			sg.attrTrees[attr.name] = NewRangeAttributeTree(attr)
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

	if len(sg.attrTrees) == 0 {
		sg.simpleList = append(sg.simpleList, protoSub)
	} else {
		sg.AddToRangeTrees(protoSub)
	}

	return nil
}

// RemoveSub
func (sg *SubGroupView) RemoveSub(sub *pb.PremiumSubscription) error {

	p, err := NewPredicate(sub.OwnPredicate)
	if err != nil {
		return err
	}

	subData := &SubData{
		addr: sub.Addr,
		pred: p,
	}

	if len(sg.attrTrees) == 0 {
		sg.RemoveSubFromList(subData)
	} else {
		sg.RemoveFromRangeTrees(subData)
	}

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

// RemoveFromRangeTrees
func (sg *SubGroupView) RemoveFromRangeTrees(sub *SubData) {
	for attr, tree := range sg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.RemoveSubFromTreeRoot(sub)
		} else {
			tree.DeleteSubFromTree(sub)
		}
	}
}

// RemoveSubFromList
func (sg *SubGroupView) RemoveSubFromList(sub *SubData) {

	for i, s := range sg.simpleList {
		if s.addr == sub.addr {
			if i == 0 {
				sg.simpleList = sg.simpleList[1:]
			} else if i+1 == len(sg.simpleList) {
				sg.simpleList = sg.simpleList[:i]
			} else {
				sg.simpleList = append(sg.simpleList[:i], sg.simpleList[i+1:]...)
			}
		}
	}
}

// AddrsToPublishEvent
func (sg *SubGroupView) AddrsToPublishEvent(p *Predicate) []*SubData {

	if len(sg.attrTrees) == 0 {
		return sg.simpleList
	} else {
		var interested []*SubData = nil
		for attr, tree := range sg.attrTrees {
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

		return interested
	}
}
