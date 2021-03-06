package contentpubsub

import (
	"context"
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
	"google.golang.org/grpc"
)

// MulticastGroup contains all suport metadata for a
// Premium publisher proper functioning
type MulticastGroup struct {
	maxSubReg int
	powerSubs int

	selfAddr       string
	predicate      *Predicate
	subByPlace     map[string]*RegionData
	trackHelp      map[string]*HelperTracker
	helpers        []*SubData
	attrTrees      map[string]*RangeAttributeTree
	simpleList     []*SubData
	lock           *sync.RWMutex
	failedDelivery chan []*SubData
}

func NewMulticastGroup(p *Predicate, addr string, maxSubReg int, powerSubs int) *MulticastGroup {

	mg := &MulticastGroup{
		maxSubReg:      maxSubReg,
		powerSubs:      powerSubs,
		selfAddr:       addr,
		predicate:      p,
		subByPlace:     make(map[string]*RegionData),
		trackHelp:      make(map[string]*HelperTracker),
		attrTrees:      make(map[string]*RangeAttributeTree),
		lock:           &sync.RWMutex{},
		failedDelivery: make(chan []*SubData),
	}

	for _, attr := range p.attributes {
		if attr.attrType == Range {
			mg.attrTrees[attr.name] = NewRangeAttributeTree(attr)
		}
	}

	return mg
}

type SubData struct {
	pred     *Predicate
	addr     string
	capacity int
	region   string
}

type ByCapacity []*SubData

func (a ByCapacity) Len() int           { return len(a) }
func (a ByCapacity) Less(i, j int) bool { return a[i].capacity > a[j].capacity }
func (a ByCapacity) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type RegionData struct {
	subs     []*SubData
	unhelped int
	helpers  []*SubData
	lock     *sync.RWMutex
}

type HelperTracker struct {
	helper        *SubData
	subsDelegated []*SubData
	remainCap     int
}

// AddSubToGroup is used to add a sub to that multicastGroup. This function analyzes the current state
// of the Group and decides if the publisher should simply add the sub to his infrastructure, recruit
// a sub to help him and delegate the sub and others to that new helper or simply delegate the sub
// to a node that is already helping him and still can receive more. To utilize helpers properly the
// subs are oredered in geografical regions to minimize latency between helper nodes and its delegated nodes
func (mg *MulticastGroup) AddSubToGroup(addr string, cap int, region string, pred *Predicate) error {

	newSub := &SubData{
		pred:     pred,
		addr:     addr,
		capacity: cap,
		region:   region,
	}

	if _, ok := mg.subByPlace[region]; !ok {
		subReg := &RegionData{unhelped: 0, lock: &sync.RWMutex{}}
		mg.subByPlace[region] = subReg
	}

	subReg := mg.subByPlace[region]
	subReg.lock.Lock()
	defer subReg.lock.Unlock()

	if len(subReg.helpers) != 0 && mg.trackHelp[subReg.helpers[len(subReg.helpers)-1].addr].remainCap > 0 {
		lastHelper := subReg.helpers[len(subReg.helpers)-1]
		mg.AddSubToHelper(newSub, lastHelper.addr)
	} else {
		if subReg.unhelped+1 > mg.maxSubReg {
			candidate := subReg.subs[0]
			var indexCutter int
			if candidate.capacity > subReg.unhelped-mg.powerSubs+1 {
				indexCutter = mg.powerSubs + 1
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

// RemoveSubFromGroup removes a sub from the multicastGroup whether it is delegated
// to a helper, a helper or a node of its responsability. In the helper case the
// subs delegated to him must be supported by the publisher, a new helper or a
// existing one. On the case of a sub unsubscribing that was delegated to a helper
// here the publisher removes it from its Group data and informs the helper for
// him to also remove that sub
func (mg *MulticastGroup) RemoveSubFromGroup(sub *pb.PremiumSubscription) error {

	mg.subByPlace[sub.Region].lock.Lock()
	defer mg.subByPlace[sub.Region].lock.Unlock()

	for i, s := range mg.subByPlace[sub.Region].subs {
		if s.addr == sub.Addr {
			toRemove := s
			if i == 0 && len(mg.subByPlace[sub.Region].subs) == 1 {
				mg.subByPlace[sub.Region].subs = nil
			} else if i == 0 {
				mg.subByPlace[sub.Region].subs = mg.subByPlace[sub.Region].subs[1:]
			} else if i+1 == len(mg.subByPlace[sub.Region].subs) {
				mg.subByPlace[sub.Region].subs = mg.subByPlace[sub.Region].subs[:i]
			} else {
				mg.subByPlace[sub.Region].subs = append(mg.subByPlace[sub.Region].subs[:i],
					mg.subByPlace[sub.Region].subs[i+1:]...)
			}

			mg.subByPlace[sub.Region].unhelped--
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

			mg.subByPlace[sub.Region].lock.Unlock()
			for _, sub := range helperFailedSubs {
				mg.AddSubToGroup(sub.addr, sub.capacity, sub.region, sub.pred)
			}
			mg.subByPlace[sub.Region].lock.Lock()

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	for _, helper := range mg.subByPlace[sub.Region].helpers {
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
				if err != nil || !ack.State {
					return errors.New("failed to unsubscribe from helper")
				}

				if i == 0 && len(mg.trackHelp[helper.addr].subsDelegated) == 1 {
					mg.trackHelp[helper.addr].subsDelegated = nil
				} else if i == 0 {
					mg.trackHelp[helper.addr].subsDelegated = mg.trackHelp[helper.addr].subsDelegated[1:]
				} else if i+1 == len(mg.subByPlace[sub.Region].subs) {
					mg.trackHelp[helper.addr].subsDelegated = mg.trackHelp[helper.addr].subsDelegated[:i]
				} else {
					mg.trackHelp[helper.addr].subsDelegated = append(mg.trackHelp[helper.addr].subsDelegated[:i],
						mg.trackHelp[helper.addr].subsDelegated[i+1:]...)
				}

				mg.trackHelp[helper.addr].helper.capacity++
				return nil
			}
		}
	}

	return nil
}

// RecruitHelper requests a helper to provide support to some subs of the group
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

// AddSubToHelper delegates a sub to an existing helper of the group
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
	if err != nil || !ack.State {
		return err
	}

	mg.trackHelp[addr].subsDelegated = append(mg.trackHelp[addr].subsDelegated, sub)
	mg.trackHelp[addr].remainCap--

	return nil
}

// AddToRangeTrees simply adds the subs to the multicast fetching-structure so that
// when publishing a event interested nodes may be efficiently recovered
func (mg *MulticastGroup) AddToRangeTrees(sub *SubData) {
	for attr, tree := range mg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.AddSubToTreeRoot(sub)
		} else {
			tree.AddSubToTree(sub)
		}
	}
}

// RemoveFromRangeTrees removes subs from the multicast fetching-structure
func (mg *MulticastGroup) RemoveFromRangeTrees(sub *SubData) {
	for attr, tree := range mg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.RemoveSubFromTreeRoot(sub)
		} else {
			tree.DeleteSubFromTree(sub)
		}
	}
}

// RemoveSubFromList removes subs from the fetching list
func (mg *MulticastGroup) RemoveSubFromList(sub *SubData) {

	for i, s := range mg.simpleList {
		if s.addr == sub.addr {
			if i == 0 && len(mg.simpleList) == 1 {
				mg.simpleList = nil
			} else if i == 0 {
				mg.simpleList = mg.simpleList[1:]
			} else if i+1 == len(mg.simpleList) {
				mg.simpleList = mg.simpleList[:i]
			} else {
				mg.simpleList = append(mg.simpleList[:i], mg.simpleList[i+1:]...)
			}

			break
		}
	}
}

// AddrsToPublishEvent returns all the subs within the publisher
// responsability that are interested in a certain event
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

// StopDelegating erases data-structure of a unsubscribed/failed helper
func (mg *MulticastGroup) StopDelegating(tracker *HelperTracker, add bool) {

	for i := 0; i < len(mg.subByPlace[tracker.helper.region].helpers); i++ {
		if mg.subByPlace[tracker.helper.region].helpers[i].addr == tracker.helper.addr {
			if i == 0 && len(mg.subByPlace[tracker.helper.region].helpers) == 1 {
				mg.subByPlace[tracker.helper.region].helpers = nil
			} else if i == 0 {
				mg.subByPlace[tracker.helper.region].helpers = mg.subByPlace[tracker.helper.region].helpers[1:]
			} else if len(mg.subByPlace[tracker.helper.region].helpers) == i+1 {
				mg.subByPlace[tracker.helper.region].helpers = mg.subByPlace[tracker.helper.region].helpers[:i]
			} else {
				mg.subByPlace[tracker.helper.region].helpers = append(mg.subByPlace[tracker.helper.region].helpers[:i],
					mg.subByPlace[tracker.helper.region].helpers[i+1:]...)
			}

			break
		}
	}

	delete(mg.trackHelp, tracker.helper.addr)

	if add {
		tracker.helper.capacity = 0
	}
}

// SubGroupView is where helper keeps
// track of its delegated peers
type SubGroupView struct {
	maxAttr    int
	pubAddr    string
	predicate  *Predicate
	helping    bool
	attrTrees  map[string]*RangeAttributeTree
	simpleList []*SubData
}

// SetHasHelper sets a previous normal sub to a helper and
// adds subs to its responsability
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

// AddSub adds a subs to a helper responsability
func (sg *SubGroupView) AddSub(sub *pb.MinimalSubData) error {

	p, err := NewPredicate(sub.Predicate, sg.maxAttr)
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

// RemoveSub removes a subs from the helper responsability
func (sg *SubGroupView) RemoveSub(sub *pb.PremiumSubscription) error {

	p, err := NewPredicate(sub.OwnPredicate, sg.maxAttr)
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

// AddToRangeTrees adds the subs to the helper fetching-structure tree
func (sg *SubGroupView) AddToRangeTrees(sub *SubData) {
	for attr, tree := range sg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.AddSubToTreeRoot(sub)
		} else {
			tree.AddSubToTree(sub)
		}
	}
}

// RemoveFromRangeTrees remove a sub from the helper fetching-structure tree
func (sg *SubGroupView) RemoveFromRangeTrees(sub *SubData) {
	for attr, tree := range sg.attrTrees {
		if _, ok := sub.pred.attributes[attr]; !ok {
			tree.RemoveSubFromTreeRoot(sub)
		} else {
			tree.DeleteSubFromTree(sub)
		}
	}
}

// RemoveSubFromList remove a sub from the helper fetching-structure list
func (sg *SubGroupView) RemoveSubFromList(sub *SubData) {

	for i, s := range sg.simpleList {
		if s.addr == sub.addr {
			if i == 0 && len(sg.simpleList) == 1 {
				sg.simpleList = nil
			} else if i == 0 {
				sg.simpleList = sg.simpleList[1:]
			} else if i+1 == len(sg.simpleList) {
				sg.simpleList = sg.simpleList[:i]
			} else {
				sg.simpleList = append(sg.simpleList[:i], sg.simpleList[i+1:]...)
			}
		}
	}
}

// AddrsToPublishEvent fetches the interested subs on a event the helper has received
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
