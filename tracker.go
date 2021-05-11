package contentpubsub

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pedroaston/contentpubsub/pb"
	"google.golang.org/grpc"
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

// NewTracker initiates a new tracker struct
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

// returnUnAckedEvents returns which event pathways haven't confirmed event
// delivery and warns the Rv of which are they and for which events
func (t *Tracker) returnUnAckedEvents() {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.Dial(t.rvAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	stream, err := client.ResendEvent(ctx)
	if err != nil {
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

			stream.Send(eL)
		}
	}

	ack, err := stream.CloseAndRecv()
	if err != nil || !ack.State {
		return
	}

}

type EventLedger struct {
	eventID      string
	event        *pb.Event
	eventLog     map[string]bool
	expectedAcks int
	receivedAcks int
	old          bool
	addrToAck    string
}

// NewEventLedger
func NewEventLedger(eID string, log map[string]bool, addr string, e *pb.Event) *EventLedger {

	eL := &EventLedger{
		eventID:      eID,
		event:        e,
		eventLog:     log,
		expectedAcks: len(log),
		receivedAcks: 0,
		old:          false,
		addrToAck:    addr,
	}

	return eL
}
