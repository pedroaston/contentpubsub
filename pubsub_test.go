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
func TestPubSubServerComms(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)
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
func TestSimpleUnsubscribing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)
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
func TestSimplePublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 3)
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
func TestSubscriptionForwarding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 7)
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
// working with failures in different situations
func TestSimpleFaultTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[2], dhts[4])

	fmt.Println("Start Tolerance Test!")
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

// TestBackupReplacement
func TestBackupReplacement(t *testing.T) {
	fmt.Println("Starting TestBackupReplacement!")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 6)

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

// TestAproxRealSubscriptionScenario with 100 peers randomly connected to
// each other, where half subscribe to one topic and the rest to another
func TestAproxRealSubscriptionScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 100)
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
