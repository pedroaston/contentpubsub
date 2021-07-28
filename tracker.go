package contentpubsub

import (
	"fmt"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
)

// Tracker keeps "track" of a rendezvous forwarded events'
// acknowledge chain, so that if after a certain time
// not all confirmation were received he forwards a resend
// request to certain peers back to the rendezvous
type Tracker struct {
	leader      bool
	attr        string
	rvPubSub    *PubSub
	eventStats  map[string]*EventLedger
	addEventAck chan *pb.EventAck
	addEventLog chan *EventLedger
	checkEvents *time.Ticker
	buffedAcks  []*pb.EventAck
}

func NewTracker(leader bool, attr string, ps *PubSub, timeToCheckDelivery time.Duration) *Tracker {

	t := &Tracker{
		leader:      leader,
		attr:        attr,
		rvPubSub:    ps,
		eventStats:  make(map[string]*EventLedger),
		addEventAck: make(chan *pb.EventAck, 8),
		addEventLog: make(chan *EventLedger, 8),
		checkEvents: time.NewTicker(timeToCheckDelivery * time.Second),
	}

	go t.trackerLoop()

	return t
}

// trackerloop processes incoming messages from the
// Rv and warns him about unacknowledged events
func (t *Tracker) trackerLoop() {

	for {
		select {
		case pid := <-t.addEventLog:
			t.newEventToCheck(pid)
		case pid := <-t.addEventAck:
			t.addAckToLedger(pid)
		case <-t.checkEvents.C:
			t.applyBuffedAcks()
			if t.leader {
				t.returnUnAckedEvents()
			}
		}
	}
}

// newEventToCheck places a event on tracker checking list
func (t *Tracker) newEventToCheck(eL *EventLedger) {
	t.eventStats[eL.eventID] = eL
}

// addAckToLedger records an acknowledge in the respective event ledger
func (t *Tracker) addAckToLedger(ack *pb.EventAck) {

	eID := fmt.Sprintf("%s%d%d", ack.EventID.PublisherID, ack.EventID.SessionNumber, ack.EventID.SeqID)
	if _, ok := t.eventStats[eID]; !ok {
		t.buffedAcks = append(t.buffedAcks, ack)
		return
	} else if _, ok := t.eventStats[eID].eventLog[ack.PeerID]; !ok {
		return
	}

	t.eventStats[eID].eventLog[ack.PeerID] = true
	t.eventStats[eID].receivedAcks++

	if t.eventStats[eID].receivedAcks == t.eventStats[eID].expectedAcks {
		delete(t.eventStats, eID)
	}
}

// applyBuffedAcks tries to apply again previous acks that
// may be left being due to delay of the Rv to send the Log
func (t *Tracker) applyBuffedAcks() {

	for _, ack := range t.buffedAcks {
		t.addAckToLedger(ack)
	}

	t.buffedAcks = nil
}

// returnUnAckedEvents returns which event pathways haven't confirmed event
// delivery and warns the Rv of which are they and for which events
func (t *Tracker) returnUnAckedEvents() {

	if len(t.eventStats) == 0 {
		return
	}

	for _, l := range t.eventStats {
		if !l.old {
			l.old = true
		} else {
			stillMissAck := make(map[string]bool)
			for peer, ack := range l.eventLog {
				if !ack {
					stillMissAck[peer] = false
				}
			}

			eL := &pb.EventLog{
				RvID:  t.attr,
				Log:   stillMissAck,
				Event: l.event,
			}

			t.rvPubSub.resendEvent(eL)
		}
	}
}

// EventLedger keeps track of all acknowledge
// received for a specific event
type EventLedger struct {
	eventID             string
	event               *pb.Event
	eventLog            map[string]bool
	expectedAcks        int
	receivedAcks        int
	old                 bool
	addrToAck           string
	originalDestination string
}

func NewEventLedger(eID string, log map[string]bool, addr string, e *pb.Event, dest string) *EventLedger {

	eL := &EventLedger{
		eventID:             eID,
		event:               e,
		eventLog:            log,
		expectedAcks:        len(log),
		receivedAcks:        0,
		old:                 false,
		addrToAck:           addr,
		originalDestination: dest,
	}

	return eL
}
