package contentpubsub

import (
	"fmt"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
)

const secondsToCheckEventDelivery = 30

type Tracker struct {
	leader      bool
	fresh       bool
	attr        string
	rvAddr      string
	timeOfBirth string
	eventStats  map[string]*EventLedger
	addEventAck chan *pb.EventAck
	addEventLog chan *EventLedger
	checkEvents *time.Ticker
}

// NewTracker
func NewTracker(leader bool, attr string, rvAddr string) *Tracker {

	t := &Tracker{
		leader:      leader,
		fresh:       true,
		attr:        attr,
		rvAddr:      rvAddr,
		timeOfBirth: time.Now().Format(time.StampMilli),
		eventStats:  make(map[string]*EventLedger),
		addEventAck: make(chan *pb.EventAck, 8),
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
func (t *Tracker) addAckToLedger(ack *pb.EventAck) {

	eID := fmt.Sprintf("%s%d%d", ack.EventID.PublisherID, ack.EventID.SessionNumber, ack.EventID.SeqID)

	if _, ok := t.eventStats[eID]; !ok {
		fmt.Println("Already acked")
		return
	} else if _, ok := t.eventStats[eID].eventLog[ack.PeerID]; !ok {
		fmt.Println("Not possible to ack")
		return
	}

	t.eventStats[eID].eventLog[ack.PeerID] = true
	t.eventStats[eID].receivedAcks++

	if t.eventStats[eID].receivedAcks == t.eventStats[eID].expectedAcks {
		delete(t.eventStats, eID)
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
	addrToAck    string
}

// NewEventLedger
func NewEventLedger(eID string, log map[string]bool, addr string) *EventLedger {

	el := &EventLedger{
		eventID:      eID,
		eventLog:     log,
		expectedAcks: len(log),
		receivedAcks: 0,
		old:          false,
		addrToAck:    addr,
	}

	return el
}
