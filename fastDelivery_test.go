package contentpubsub

import (
	"context"
	"testing"
	"time"
)

// TestSimpleFastDelivery
func TestSimpleFastDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[4].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].MyPremiumPublish("portugal T", "Portugal is great!", "portugal T")

	time.Sleep(time.Second)
}

// TestSimpleFastDelivery
// INCOMPLETE
func TestSimpleFastDeliveryWithRanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("wine T/year R 1990 2000")
	pubsubs[1].MyPremiumSubscribe("wine T/year R 2000 2000", pubsubs[0].serverAddr, "wine T/year R 1990 2000", 10)
	pubsubs[2].MyPremiumSubscribe("wine T/year R 1990 2000", pubsubs[0].serverAddr, "wine T/year R 1990 2000", 20)
	pubsubs[3].MyPremiumSubscribe("wine T/year R 1992 1997", pubsubs[0].serverAddr, "wine T/year R 1990 2000", 10)
	pubsubs[4].MyPremiumSubscribe("wine T/year R 1993 1995", pubsubs[0].serverAddr, "wine T/year R 1990 2000", 10)
	pubsubs[0].MyPremiumPublish("wine T/year R 1990 2000", "Porto wines from 1999 are rarer", "wine T/year R 1999 1999")

	time.Sleep(time.Second)
}

// TestSimpleFastDeliveryWithHelper
func TestSimpleFastDeliveryWithHelper(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 8)
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	var pubsubs [8]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 20)
	pubsubs[4].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[5].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[6].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[7].MyPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].MyPremiumPublish("portugal T", "Portugal is great!", "portugal T")

	time.Sleep(2 * time.Second)
}
