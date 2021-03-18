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

	err := pubsubs[0].MySubscribe("chocolate T")

	if err != nil {
		t.Fatal("Failed Subscription")
	}

	time.Sleep(5000000000)

}
