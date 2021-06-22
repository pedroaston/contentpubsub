package contentpubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// #################################################################
// ## DISCLAMER: these tests can only be confired by checking     ##
// ##            their output and they should be run individually ##
// ##            for reasons of port and ID assigning             ##
// #################################################################

// TestSimpleUnsubscribing just subscribes to certain
// events and then unsubscribes to them
// Test composition: 2 nodes
// >> 1 Subscriber that subscribes and unsubscribes
// to a kind of event
func TestSimpleUnsubscribing(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleUnsubscribing $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])

	var pubsubs [2]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	err := pubsubs[0].MySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if len(pubsubs[0].myFilters.filters[1]) != 1 {
		t.Fatal("Error Subscribing!")
	}

	pubsubs[0].MyUnsubscribe("portugal T")
	if len(pubsubs[0].myFilters.filters[1]) != 0 {
		t.Fatal("Failed Unsubscribing!")
	}
}

// TestSimplePublish simply subscribes to a event and then publishes it
// Test composition: 3 nodes
// >> 1 Publisher that publishes a event
// >> 2 Subscribers that subscribe to a event
func TestSimplePublish(t *testing.T) {
	fmt.Printf("\n$$$ TestSimplePublish $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 3)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])

	var pubsubs [3]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[0].MySubscribe("portugal T")
	pubsubs[1].MySubscribe("portugal T")

	time.Sleep(250 * time.Millisecond)

	err := pubsubs[2].MyPublish("Portugal is beautifull!", "portugal T")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond)
}

// TestSubscriptionForwarding attemps to see if the subscription
// travels to several nodes until it reaches the rendezvous
// Test composition: 7 nodes
// >> 4 Subscribers subscribing to different kinds of events
// >> 3 Passive nodes that just diffuse the subscriptions
func TestSubscriptionForwarding(t *testing.T) {
	fmt.Printf("\n$$$ TestSubscriptionForwarding $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 7)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[2], dhts[1])
	connect(t, ctx, dhts[1], dhts[3])
	connect(t, ctx, dhts[4], dhts[1])
	connect(t, ctx, dhts[1], dhts[5])
	connect(t, ctx, dhts[6], dhts[1])

	var pubsubs [7]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	if pubsubs[0].MySubscribe("chocolate T") != nil {
		t.Fatal("Failed 1st Subscription")
	}

	if pubsubs[0].MySubscribe("soccer T/goals R 2 5") != nil {
		t.Fatal("Failed 2nd Subscription")
	}

	if pubsubs[0].MySubscribe("portugal T") != nil {
		t.Fatal("Failed 3rd Subscription")
	}

	if pubsubs[4].MySubscribe("portugal T") != nil {
		t.Fatal("Failed 4th Subscription")
	}

	time.Sleep(250 * time.Millisecond)
}

// TestSimpleFaultTolerance just tries to show the system
// working with failures in different situations and
// maitaining its functioning
// Test composition: 5 nodes
// >> 1 Publisher publishes a kind of event
// >> 1 Subscriber subscribing a kind of event
// >> 3 Passive nodes that just diffuse the subscriptions
// being one of the intermediate nodes of the subscription
// chain terminated
func TestSimpleFaultTolerance(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFaultTolerance $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 6)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[2], dhts[4])
	connect(t, ctx, dhts[2], dhts[5])

	var pubsubs [6]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[0].MySubscribe("portugal T")
	pubsubs[5].MySubscribe("portugal T")
	time.Sleep(250 * time.Millisecond)

	pubsubs[1].TerminateService()
	time.Sleep(250 * time.Millisecond)

	pubsubs[4].MyPublish("valmitão tem as melhores marolas do mundo!", "portugal T")
	time.Sleep(250 * time.Millisecond)
}

// TestBackupReplacement shows the scoutsubs overlay reation
// once a backup node crashes, and the substitution process
// Test composition: 6 nodes
// >> 1 Subscribers subscribing to a kind of event
// >> 5 Passive nodes that just diffuse the subscriptions
// being one the backups of the subscriber replaced once
// one of its backups fails
// OBSERVATIONS:
// >> backup failed may only be detected after timeout
// and stop the entire subscription process, and for that
// I reduced the updateMyBackups to 2 seconds
func TestBackupReplacement(t *testing.T) {
	fmt.Printf("\n$$$ TestBackupReplacement $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 6)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])

	var pubsubs [6]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[3].MySubscribe("benfica T")
	time.Sleep(time.Second)
	pubsubs[2].TerminateService()
	time.Sleep(time.Second)
	pubsubs[1].MySubscribe("benfica T")
	time.Sleep(3 * time.Second)
}

// TestRefreshRoutine is used to show how the filter tables
// and advertising boards are periodically replaced
// Test composition: 6 nodes
// >> 2 Stadard Subscriber and 1 unsubscribes and the
// other makes a group search
// >> 1 Premium Publisher & Standard Subscriber that fails
// >> 1 Premium Publisher
// >> 1 Standard Publisher that publishes two kinds of events
// >> 3 Passive nodes that just diffuse the subscriptions
// To experience this test must switch refresh rate to 2 seconds
func TestRefreshRoutine(t *testing.T) {
	fmt.Printf("\n$$$ TestRefreshRoutine $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 6)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])

	var pubsubs [6]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[4].CreateMulticastGroup("portugal T")
	pubsubs[1].MySubscribe("portugal T")
	pubsubs[2].MySubscribe("portugal T")
	pubsubs[2].CreateMulticastGroup("portugal T")
	pubsubs[1].MyUnsubscribe("portugal T")
	pubsubs[3].MySubscribe("bali T")
	pubsubs[2].TerminateService()

	time.Sleep(5 * time.Second)

	pubsubs[0].MyPublish("bali has some good waves", "bali T")
	pubsubs[0].MyPublish("portugal has epic waves", "portugal T")
	time.Sleep(time.Second)
}

// TestRedirectMechanism shows that a event my jump several hops on
// the network if those intermidiate nodes don't lead to more subscribers and
// finishes the test by printing the latency on the event delivery
// Test composition: 4 nodes
// >> 1 Subscriber subscribes at the bottom of the dissemination chain
// >> 1 Publisher publishing a event next to the dissemination chain
func TestRedirectMechanism(t *testing.T) {
	fmt.Printf("\n$$$ TestRedirectMechanism $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])

	var pubsubs [4]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[0].MySubscribe("portugal T")

	time.Sleep(250 * time.Millisecond)

	pubsubs[3].MyPublish("Portugal sometime can be the best!", "portugal T")

	time.Sleep(250 * time.Millisecond)
}

// TestReliableEventDelivery proves that the rv tracker leader
// warns the rv node to retransmit a certain event to certain
// pathways were the event still has been confirmed
// Test composition: 5 nodes
// >> 1 Publisher that publishes a event
// >> 3 Subscriber that subscribe to a event and artificially
// don't ackUp for 6 seconds
// Special conditions
// >> secondsToCheckEventDelivery = 5
// >> time.Sleep(6 * Seconds) at begining of ackUp
func TestReliableEventDelivery(t *testing.T) {
	fmt.Printf("\n$$$ TestReliableEventDelivery $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])
	connect(t, ctx, dhts[1], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
	}

	pubsubs[2].MySubscribe("portugal T")
	pubsubs[3].MySubscribe("portugal T")
	pubsubs[4].MySubscribe("portugal T")

	time.Sleep(100 * time.Millisecond)

	err := pubsubs[0].MyPublish("Portugal is beautifull!", "portugal T")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(7 * time.Second)

}
