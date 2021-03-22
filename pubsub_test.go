package contentpubsub

import (
	"context"
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
		pubsubs[i] = NewPubSub(dht)
	}

	err := pubsubs[0].MySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if pubsubs[1].currentFilterTable.routes[peer.Encode(pubsubs[0].ipfsDHT.PeerID())].filters[1][0].String() != "<portugal> " {
		t.Fatal("Failed Subscription")
	}
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
		pubsubs[i] = NewPubSub(dht)
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

// TestAproxRealSubscriptionScenario with 100 peers randomly connected to
// each other, where half subscribe to one topic and the rest to another
func TestAproxRealSubscriptionScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 100)
	bootstrapDhts(t, ctx, dhts)

	var pubsubs [100]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht)
	}

	var err1, err2 error
	for i, ps := range pubsubs {
		if i%2 == 0 {
			err1 = ps.MySubscribe("portugal T/soccer T")
		} else {
			err2 = ps.MySubscribe("tesla T/stock T/value R 500 800")
		}

		if err1 != nil || err2 != nil {
			t.Fatal("Error Subscribing in mass")
		}
	}

	time.Sleep(2 * time.Second)
}
