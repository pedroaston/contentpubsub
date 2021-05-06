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
	leader      bool
	fresh       bool
	timeOfBirth string
	eventStats  map[string]*EventLedger
	addEventAck chan *EventAck
	addEventLog chan *EventLedger
	checkEvents *time.Ticker
}

// NewTracker
func NewTracker(leader bool) *Tracker {

	t := &Tracker{
		leader:      leader,
		fresh:       true,
		timeOfBirth: time.Now().Format(time.StampMilli),
		eventStats:  make(map[string]*EventLedger),
		addEventAck: make(chan *EventAck, 8),
		addEventLog: make(chan *EventLedger, 8),
		checkEvents: time.NewTicker(secondsToCheckEventDelivery * time.Second),
	}

	go t.trackerLoop()

	return t
}

// trackerloop
func (t *Tracker) trackerLoop() {

	for {
		select {
		case pid := <-t.addEventLog:
			t.newEventToCheck(pid)
		case pid := <-t.addEventAck:
			t.addAckToLedger(pid)
		case <-t.checkEvents.C:
			if t.leader {
				t.returnUnAckedEvents()
			}
		}
	}
}

// newEventToCheck
func (t *Tracker) newEventToCheck(eL *EventLedger) {

	t.eventStats[eL.eventID] = eL
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
	eventID      string
	eventLog     map[string]bool
	expectedAcks int
	receivedAcks int
	old          bool
}

// NewEventLedger
func NewEventLedger(eID string, log map[string]bool) *EventLedger {

	el := &EventLedger{
		eventID:      eID,
		eventLog:     log,
		expectedAcks: len(log),
		receivedAcks: 0,
		old:          false,
	}

	return el
}
