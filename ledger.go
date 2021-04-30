package contentpubsub

import (
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
func (r *HistoryRecord) SaveReceivedEvent(event *pb.Event, eventSource string, protocol string) {

	aux, err := time.Parse(time.StampMilli, event.BirthTime)
	if err != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  eventSource,
		timeOfTravel: time.Since(aux),
		eventData:    event.Event,
		protocol:     protocol,
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}
