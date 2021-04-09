package contentpubsub

import (
	"context"
	"testing"
	"time"
)

// TestSimpleFastDelivery
// INCOMPLETE
func TestSimpleFastDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)
	connect(t, ctx, dhts[0], dhts[1])

	var pubsubs [2]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T")
	pubsubs[0].MyPremiumPublish("portugal T", "Portugal is great!", "portugal T")
}
