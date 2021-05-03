package contentpubsub

import (
	"fmt"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
)

type HistoryRecord struct {
	receivedEvents   []*EventRecord
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

	aux, err := time.Parse(time.StampMilli, event.BirthTime)
	if err != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  event.EventID.PublisherID,
		timeOfTravel: time.Since(aux),
		eventData:    event.Event,
		protocol:     "ScoutSubs",
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}

// SaveReceivedEvent
func (r *HistoryRecord) SaveReceivedPremiumEvent(event *pb.PremiumEvent) {

	aux, err := time.Parse(time.StampMilli, event.BirthTime)
	if err != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  event.GroupID.OwnerAddr,
		timeOfTravel: time.Since(aux),
		eventData:    event.Event,
		protocol:     "FastDelivery",
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}

// CompileLatencyResults
func (r *HistoryRecord) CompileLatencyResults() string {

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

	avgScoutLatency := scoutLatencySum / len(r.receivedEvents)
	avgFastLatency := fastLatencySum / len(r.receivedEvents)

	scoutMetrics := fmt.Sprintf("Received %d ScoutSubs events with an avg latency of %d ms\n", scoutEvents, avgScoutLatency)
	fastMetrics := fmt.Sprintf("Received %d FastDelivery events with an avg latency of %d ms\n", fastEvents, avgFastLatency)

	return scoutMetrics + fastMetrics
}
