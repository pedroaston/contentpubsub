package contentpubsub

import (
	"time"

	"github.com/pedroaston/contentpubsub/pb"
)

type HistoryRecord struct {
	receivedEvents   []*EventRecord
	timeToSub        []int
	operationHistory map[string]int
}

type EventRecord struct {
	eventSource  string
	eventData    string
	protocol     string
	timeOfTravel time.Duration
}

// NewHistoryRecord
func NewHistoryRecord() *HistoryRecord {
	record := &HistoryRecord{operationHistory: make(map[string]int)}

	return record
}

// AddOperationStat
func (r *HistoryRecord) AddOperationStat(opName string) {

	if _, ok := r.operationHistory[opName]; !ok {
		r.operationHistory[opName] = 1
	} else {
		r.operationHistory[opName]++
	}
}

// SaveReceivedEvent
func (r *HistoryRecord) SaveReceivedEvent(event *pb.Event) {

	past, err1 := time.Parse(time.StampMilli, event.BirthTime)
	if err1 != nil {
		return
	}

	present, err2 := time.Parse(time.StampMilli, time.Now().Format(time.StampMilli))
	if err2 != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  event.EventID.PublisherID,
		timeOfTravel: present.Sub(past),
		eventData:    event.Event,
		protocol:     "ScoutSubs",
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}

// SaveReceivedEvent
func (r *HistoryRecord) SaveReceivedPremiumEvent(event *pb.PremiumEvent) {

	past, err1 := time.Parse(time.StampMilli, event.BirthTime)
	if err1 != nil {
		return
	}

	present, err2 := time.Parse(time.StampMilli, time.Now().Format(time.StampMilli))
	if err2 != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  event.GroupID.OwnerAddr,
		timeOfTravel: present.Sub(past),
		eventData:    event.Event,
		protocol:     "FastDelivery",
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}

// SaveTimeToSub
func (r *HistoryRecord) SaveTimeToSub(start string) {

	past, err1 := time.Parse(time.StampMilli, start)
	if err1 != nil {
		return
	}

	present, err2 := time.Parse(time.StampMilli, time.Now().Format(time.StampMilli))
	if err2 != nil {
		return
	}

	r.timeToSub = append(r.timeToSub, int(present.Sub(past).Milliseconds()))
}

// CompileAvgTimeToSub
func (r *HistoryRecord) CompileAvgTimeToSub() int {

	sum := 0
	for _, t := range r.timeToSub {
		sum += t
	}

	if len(r.timeToSub) == 0 {
		return 0
	} else {
		return sum / len(r.timeToSub)
	}
}

// CompileLatencyResults
func (r *HistoryRecord) CompileLatencyResults() (int, int, int, int) {

	var scoutLatencySum int = 0
	var scoutEvents int = 0
	var fastLatencySum int = 0
	var fastEvents int = 0
	for _, e := range r.receivedEvents {
		if e.protocol == "ScoutSubs" {
			scoutEvents++
			scoutLatencySum += int(e.timeOfTravel.Milliseconds())
		} else {
			fastEvents++
			fastLatencySum += int(e.timeOfTravel.Milliseconds())
		}
	}

	var avgScoutLatency int
	if scoutEvents == 0 {
		avgScoutLatency = 0
	} else {
		avgScoutLatency = scoutLatencySum / len(r.receivedEvents)
	}

	var avgFastLatency int
	if fastEvents == 0 {
		avgFastLatency = 0
	} else {
		avgFastLatency = fastLatencySum / len(r.receivedEvents)
	}

	return scoutEvents, fastEvents, avgScoutLatency, avgFastLatency
}
