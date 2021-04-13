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

// TestSimpleFastDeliveryWithRanges
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

	pubsubs[0].CreateMulticastGroup("wine T/year R 1990 1997")
	pubsubs[1].MyPremiumSubscribe("wine T/year R 1991 1994", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[2].MyPremiumSubscribe("wine T/year R 1990 1997", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 20)
	pubsubs[3].MyPremiumSubscribe("wine T/year R 1992 1997", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[4].MyPremiumSubscribe("wine T/year R 1993 1995", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[0].MyPremiumPublish("wine T/year R 1990 1997", "Porto wines from 1996 are rarer", "wine T/year R 1996 1996")

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

	time.Sleep(time.Second)
}

// TestSimpleFastDeliveryUnsubscribe
// NOT PASSING
func TestSimpleFastDeliveryUnsubscribe(t *testing.T) {
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
	time.Sleep(time.Second)

	pubsubs[1].MyPremiumUnsubscribe("portugal T", pubsubs[0].serverAddr)
	pubsubs[5].MyPremiumUnsubscribe("portugal T", pubsubs[0].serverAddr)
	pubsubs[0].MyPremiumPublish("portugal T", "Portugal is really great!", "portugal T")
	time.Sleep(time.Second)
}

// TestFastDeliveryWithHelperFailure
// NOT PASSING
func TestFastDeliveryWithHelperFailure(t *testing.T) {
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
	time.Sleep(time.Second)

	pubsubs[3].Terminate()
	pubsubs[0].MyPremiumPublish("portugal T", "Portugal is really great!", "portugal T")
	time.Sleep(time.Second)
}
