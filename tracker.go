package contentpubsub

import (
	"time"
)

const secondsToCheckEventDelivery = 30

type EventAck struct {
	eventID string
	peerID  string
}

type Tracker struct {
	fresh       bool
	timeOfBirth string
	eventStats  map[string]*EventLedger
	addEventAck chan *EventAck
	checkEvents *time.Ticker
}

// NewTracker
func NewTracker(leader bool) *Tracker {

	t := &Tracker{
		fresh:       true,
		timeOfBirth: time.Now().Format(time.StampMilli),
		eventStats:  make(map[string]*EventLedger),
		addEventAck: make(chan *EventAck, 100),
		checkEvents: time.NewTicker(secondsToCheckEventDelivery * time.Second),
	}

	if leader {
		go t.trackerLoop()
	}

	return t
}

// trackerloop
func (t *Tracker) trackerLoop() {

	for {
		select {
		case pid := <-t.addEventAck:
			t.addAckToLedger(pid)
		case <-t.checkEvents.C:
			t.returnUnAckedEvents()
		}
	}
}

// newEventToCheck
func (t *Tracker) newEventToCheck(eventID string, log map[string]bool) {

	t.eventStats[eventID] = NewEventLedger(log)
}

// addAckToLedger
func (t *Tracker) addAckToLedger(ack *EventAck) {

	t.eventStats[ack.eventID].eventLog[ack.peerID] = true
	t.eventStats[ack.eventID].receivedAcks++

	if t.eventStats[ack.eventID].receivedAcks == t.eventStats[ack.eventID].expectedAcks {
		delete(t.eventStats, ack.eventID)
	}

}

// returnUnAckedEvents
// INCOMPLETE >> Need to send back to Rv
func (t *Tracker) returnUnAckedEvents() {
	stillMissAck := make(map[string][]string)

	for e, l := range t.eventStats {
		if !l.old {
			l.old = true
		} else {
			for peer, ack := range l.eventLog {
				if !ack {
					stillMissAck[e] = append(stillMissAck[e], peer)
				}
			}
		}
	}
}

type EventLedger struct {
	eventLog     map[string]bool
	expectedAcks int
	receivedAcks int
	old          bool
}

// NewEventLedger
func NewEventLedger(log map[string]bool) *EventLedger {

	el := &EventLedger{
		eventLog:     log,
		expectedAcks: len(log),
		receivedAcks: 0,
		old:          false,
	}

	return el
}
