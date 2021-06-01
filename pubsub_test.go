package contentpubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSimpleUnsubscribing just subscribes to certain
// events and then unsubscribes to them
// Test composition: 2 nodes
// >> 1 Subscriber that subscribes and unsubscribes
// to a kind of event
func TestSimpleUnsubscribing(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleUnsubscribing $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
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
		pubsubs[i] = NewPubSub(dht, "PT")
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
// Test composition: 2 nodes
// >> 1 Publisher that publishes a event
// >> 1 Subscriber that subscribe to a event
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
		pubsubs[i] = NewPubSub(dht, "PT")
	}

	pubsubs[0].MySubscribe("portugal T")
	pubsubs[1].MySubscribe("portugal T")

	time.Sleep(100 * time.Millisecond)

	err := pubsubs[2].MyPublish("Portugal is beautifull!", "portugal T")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
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
		pubsubs[i] = NewPubSub(dht, "PT")
	}

	err1 := pubsubs[0].MySubscribe("chocolate T")
	if err1 != nil {
		t.Fatal("Failed 1st Subscription")
	}

	err2 := pubsubs[0].MySubscribe("soccer T/goals R 2 5")
	if err2 != nil {
		t.Fatal("Failed 2nd Subscription")
	}

	err3 := pubsubs[0].MySubscribe("portugal T")
	if err3 != nil {
		t.Fatal("Failed 3rd Subscription")
	}

	err4 := pubsubs[4].MySubscribe("portugal T")
	if err4 != nil {
		t.Fatal("Failed 4th Subscription")
	}

	time.Sleep(time.Second)
}

// TestSimpleFaultTolerance just tries to show the system
// working with failures in different situations and
// maitaining its functioning
// Test composition: 5 nodes
// >> 1 Publisher publishes a kind of event
// >> 1 Subscriber subscribing a kind of event
// >> 3 Passive nodes that just diffuse the subscriptions
// being one of the intermidiante nodes of the subscription
// chain terminated
func TestSimpleFaultTolerance(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFaultTolerance $$$\n")

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
	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[2], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "PT")
	}

	pubsubs[0].MySubscribe("portugal T")
	time.Sleep(time.Second)

	pubsubs[1].TerminateService()
	time.Sleep(time.Second)

	pubsubs[4].MyPublish("valmitão tem as melhores marolas do mundo!", "portugal T")
	time.Sleep(time.Second)

}

// TestBackupReplacement shows the scoutsubs overlay reation
// once a backup node crashes, and the substitution process
// Test composition: 6 nodes
// >> 1 Subscribers subscribing to a kind of event
// >> 5 Passive nodes that just diffuse the subscriptions
// being one the backups of the subscriber replaced once
// one of its backups fails
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
		pubsubs[i] = NewPubSub(dht, "PT")
	}

	time.Sleep(time.Second)
	pubsubs[3].TerminateService()
	pubsubs[5].MySubscribe("soccer T")
	time.Sleep(time.Second)
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
		pubsubs[i] = NewPubSub(dht, "PT")
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
	pubsubs[3].MyGroupSearchRequest("portugal T")
	pubsubs[4].gracefullyTerminate()
}

// TestMultipleAttributeEvent is for a correctness error debug
// Test composition: 6 nodes
// Bollocks
// >> 1 Publisher that publishes a event
// >> 3 Subscriber that subscribe to a event and artificially
// don't ackUp for 6 seconds
// Special conditions
// >> secondsToCheckEventDelivery = 5
func TestMultipleAttributeEvent(t *testing.T) {
	fmt.Printf("\n$$$ TestMultipleAttributeEvent $$$\n")

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

	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])
	connect(t, ctx, dhts[1], dhts[4])
	connect(t, ctx, dhts[1], dhts[5])

	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[2], dhts[4])
	connect(t, ctx, dhts[2], dhts[5])

	connect(t, ctx, dhts[3], dhts[4])
	connect(t, ctx, dhts[3], dhts[5])

	connect(t, ctx, dhts[4], dhts[5])

	var pubsubs [6]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "PT")
	}

	pubsubs[0].MySubscribe("portugal T")
	pubsubs[1].MySubscribe("portugal T/trip T")
	pubsubs[2].MySubscribe("portugal T/trip T/surf T")
	pubsubs[3].MySubscribe("portugal T/trip T/surf T/price R 1000 1500")
	pubsubs[4].MySubscribe("trip T/surf T/price R 1000 1500")
	time.Sleep(200 * time.Millisecond)

	pubsubs[5].MyPublish("Scam message :)", "portugal T/trip T/surf T/price R 1200 1200")
	time.Sleep(200 * time.Millisecond)
}
