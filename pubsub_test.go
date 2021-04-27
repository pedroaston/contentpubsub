package contentpubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

// TestPubSubServerComms only wants to assure that pubsub servers can communicate
// by initializing both and subscribing to events about portugal
// Test composition: 2 nodes
// >> 1 Subscriber that subscribes to that event
func TestPubSubServerComms(t *testing.T) {
	fmt.Printf("\n$$$ TestPubSubServerComms $$$\n")

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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	err := pubsubs[0].mySubscribe("tenis T")

	time.Sleep(time.Second)

	if err != nil {
		t.Fatal(err)
	} else if pubsubs[1].currentFilterTable.routes[peer.Encode(pubsubs[0].ipfsDHT.PeerID())].filters[1][0].String() != "<tenis> " {
		t.Fatal("Failed Subscription")
	}
}

// TestSimpleUnsubscribing just subscribes to certain
// events and then unsubscribes to them
// Test composition: 2 nodes
// >> 1 Subscriber that subscribes and unsubscribes
// to a kind of event
func TestSimpleUnsubscribing(t *testing.T) {
	fmt.Printf("\n $$$ TestSimpleUnsubscribing $$$\n")

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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	err := pubsubs[0].mySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if len(pubsubs[0].myFilters.filters[1]) != 1 {
		t.Fatal("Error Subscribing!")
	}

	pubsubs[0].myUnsubscribe("portugal T")
	if len(pubsubs[0].myFilters.filters[1]) != 0 {
		t.Fatal("Failed Unsubscribing!")
	}
}

// TestSimplePublish simply subscribes to a event and then publishes it
// Test composition: 2 nodes
// >> 1 Publisher that publishes a event
// >> 1 Subscriber that subscribe to a event
func TestSimplePublish(t *testing.T) {
	fmt.Printf("\n $$$ TestSimplePublish $$$\n")

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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].mySubscribe("portugal T")
	pubsubs[1].mySubscribe("portugal T")

	time.Sleep(time.Second)

	err := pubsubs[2].myPublish("Portugal is beautifull!", "portugal T")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
}

// TestSubscriptionForwarding attemps to see if the subscription
// travels to several nodes until it reaches the rendezvous
// Test composition: 7 nodes
// >> 4 Subscribers subscribing to different kinds of events
// >> 3 Passive nodes that just diffuse the subscriptions
func TestSubscriptionForwarding(t *testing.T) {
	fmt.Printf("\n $$$ TestSubscriptionForwarding $$$\n")

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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	err1 := pubsubs[0].mySubscribe("chocolate T")
	if err1 != nil {
		t.Fatal("Failed 1st Subscription")
	}

	err2 := pubsubs[0].mySubscribe("soccer T/goals R 2 5")
	if err2 != nil {
		t.Fatal("Failed 2nd Subscription")
	}

	err3 := pubsubs[0].mySubscribe("portugal T")
	if err3 != nil {
		t.Fatal("Failed 3rd Subscription")
	}

	err4 := pubsubs[4].mySubscribe("portugal T")
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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].mySubscribe("portugal T")
	time.Sleep(time.Second)

	pubsubs[1].terminateService()
	time.Sleep(time.Second)

	pubsubs[4].myPublish("valmitão tem as melhores marolas do mundo!", "portugal T")
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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	time.Sleep(time.Second)
	pubsubs[3].terminateService()
	pubsubs[5].mySubscribe("soccer T")
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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[4].CreateMulticastGroup("portugal T")
	pubsubs[1].mySubscribe("portugal T")
	pubsubs[2].mySubscribe("portugal T")
	pubsubs[2].CreateMulticastGroup("portugal T")
	pubsubs[1].myUnsubscribe("portugal T")
	pubsubs[3].mySubscribe("bali T")
	pubsubs[2].terminateService()

	time.Sleep(5 * time.Second)

	pubsubs[0].myPublish("bali has some good waves", "bali T")
	pubsubs[0].myPublish("portugal has epic waves", "portugal T")
	pubsubs[3].myGroupSearchRequest("portugal T")
	pubsubs[4].gracefullyTerminate()
}

// TestRedirectMechanism shows that a event my jump several hops on
// the network if those intermidiate nodes don't lead to more subscribers
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
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].mySubscribe("portugal T")

	time.Sleep(time.Second)

	pubsubs[3].myPublish("Portugal sometime can be the best!", "portugal T")

}

// TestAproxRealSubscriptionScenario with 100 peers randomly connected to
// each other, where half subscribe to one topic and the rest to another
func TestAproxRealSubscriptionScenario(t *testing.T) {
	fmt.Printf("\n$$$ TestAproxRealSubscriptionScenario $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 100)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}
	}()

	bootstrapDhts(t, ctx, dhts)

	var pubsubs [100]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	var err1, err2 error
	for i, ps := range pubsubs {
		if i%2 == 0 {
			err1 = ps.mySubscribe("portugal T/soccer T")
		} else {
			err2 = ps.mySubscribe("tesla T/stock T/value R 500 800")
		}
		if err1 != nil || err2 != nil {
			t.Fatal("Error Subscribing in mass")
		}
	}

	time.Sleep(2 * time.Second)
}
