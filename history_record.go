package contentpubsub

import (
	"time"
)

type HistoryRecord struct {
	receivedEvents   []*EventRecord
	timeToSub        []int
	operationHistory map[string]int
}

type EventRecord struct {
	eventSource  string
	eventData    string
	timeOfTravel time.Duration
}

func NewHistoryRecord() *HistoryRecord {
	record := &HistoryRecord{operationHistory: make(map[string]int)}

	return record
}

// SaveReceivedEvent register the time a event took until it reached the subscriber
func (r *HistoryRecord) SaveReceivedEvent(eScource string, eBirth string, eData string) {

	past, err1 := time.Parse(time.StampMilli, eBirth)
	if err1 != nil {
		return
	}

	present, err2 := time.Parse(time.StampMilli, time.Now().Format(time.StampMilli))
	if err2 != nil {
		return
	}

	eventRecord := &EventRecord{
		eventSource:  eScource,
		timeOfTravel: present.Sub(past),
		eventData:    eData,
	}

	r.receivedEvents = append(r.receivedEvents, eventRecord)
}

// SaveTimeToSub register the time it took to confirm a subscription
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

// EventStats returns all events time of travel
func (r *HistoryRecord) EventStats() []int {

	var events []int

	for _, e := range r.receivedEvents {
		events = append(events, int(e.timeOfTravel.Milliseconds()))
	}

	return events
}

// CompileCorrectnessResults returns the number of events missing or received
// more than once, by comparing with a array of supposed received events
func (r *HistoryRecord) CorrectnessStats(expected []string) (int, int) {

	missed := 0
	duplicated := 0

	for _, exp := range expected {
		received := false
		for _, e := range r.receivedEvents {
			if e.eventData == exp && !received {
				received = true
			} else if e.eventData == exp {
				duplicated++
			}
		}

		if !received {
			missed++
		}
	}

	r.receivedEvents = nil
	return missed, duplicated
}
