package contentpubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSimpleFastDeliveryWithSearch tests subscription, publishing
// and advertising/search of the FastDelivery protocol
// Test composition: 5 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 4 Premium Subscribers that subscribe to a MulticastGroup
func TestSimpleFastDeliveryWithSearch(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFastDeliveryWithSearch $$$\n")

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
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[4].CreateMulticastGroup("portugal T")
	pubsubs[1].myGroupSearchRequest("portugal T")
	pubsubs[1].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[4].myPremiumPublish("portugal T", "Portugal is great!", "portugal T")

	time.Sleep(time.Second)
}

// TestSimpleFastDeliveryWithRanges is similar to the test above but instead of using
// a simple predicate it uses one composed by a topic attribute and a range one
// Test composition: 5 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 4 Premium Subscribers that subscribe to a MulticastGroup
func TestSimpleFastDeliveryWithRanges(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFastDeliveryWithRanges $$$\n")

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
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	var pubsubs [5]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("wine T/year R 1990 1997")
	pubsubs[1].myPremiumSubscribe("wine T/year R 1991 1994", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[2].myPremiumSubscribe("wine T/year R 1990 1997", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 20)
	pubsubs[3].myPremiumSubscribe("wine T/year R 1992 1997", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[4].myPremiumSubscribe("wine T/year R 1993 1995", pubsubs[0].serverAddr, "wine T/year R 1990 1997", 10)
	pubsubs[0].myPremiumPublish("wine T/year R 1990 1997", "Porto wines from 1996 are rarer", "wine T/year R 1996 1996")

	time.Sleep(time.Second)
}

// TestSimpleFastDeliveryWithHelper shows the correct dissemination of events
// when the publisher recruits a helper to assist him
// Test composition: 8 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 7 Premium Subscribers that subscribe to a MulticastGroup being one
// of them more powerfull than the others and so will become a helper
func TestSimpleFastDeliveryWithHelper(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFastDeliveryWithHelper $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 8)
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
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	var pubsubs [8]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 20)
	pubsubs[4].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[5].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[6].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[7].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is great!", "portugal T")

	time.Sleep(time.Second)
}

// TestSimpleFastDeliveryUnsubscribe shows that non-helpers can successfully unsubscribe
// the Group, stoping receiving events and not hurt the rest of the Group
// Test composition: 8 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 7 Premium Subscribers that subscribe to a MulticastGroup being one
// of them more powerfull than the others and so will become a helper
// >>>> 2 of the non-helper unsubscribe being one of the helper responsability
// and the other of the publisher responsibility
func TestSimpleFastDeliveryUnsubscribe(t *testing.T) {
	fmt.Printf("\n$$$ TestSimpleFastDeliveryUnsubscribe $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 8)
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
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	var pubsubs [8]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 20)
	pubsubs[4].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[5].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[6].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[7].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is great!", "portugal T")
	time.Sleep(time.Second)

	pubsubs[1].myPremiumUnsubscribe("portugal T", pubsubs[0].serverAddr)
	pubsubs[5].myPremiumUnsubscribe("portugal T", pubsubs[0].serverAddr)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is really great!", "portugal T")
	time.Sleep(time.Second)
}

// TestFastDeliveryHelperUnsubscribe shows that a MulticastGroup knows how
// to react to when a helper sub unsubscribes to the Group
// Test composition: 8 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 7 Premium Subscribers that subscribe to a MulticastGroup being one
// of them more powerfull than the others and so will become a helper
// and then unsubscribe
func TestFastDeliveryHelperUnsubscribe(t *testing.T) {
	fmt.Printf("\n$$$ TestFastDeliveryHelperUnsubscribe $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 8)
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
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	var pubsubs [8]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 20)
	pubsubs[4].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[5].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[6].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[7].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is great!", "portugal T")
	time.Sleep(time.Second)

	pubsubs[3].myPremiumUnsubscribe("portugal T", pubsubs[0].serverAddr)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is really great!", "portugal T")
	time.Sleep(time.Second)
}

// TestFastDeliveryWithHelperFailure shows that a MulticastGroup recovers
// from a helper node failure without losing subs and without lefting the
// subscribers without their premium events
// Test composition: 8 nodes
// >> 1 Premium Publisher that creates and publishes in a MulticastGroup
// >> 7 Premium Subscribers that subscribe to a MulticastGroup being one
// of them more powerfull than the others and so will become a helper
// and then fail abruptly
func TestFastDeliveryWithHelperFailure(t *testing.T) {
	fmt.Printf("\n$$$ TestFastDeliveryWithHelperFailure $$$\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 8)
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
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	var pubsubs [8]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, "EU", "PT")
	}

	pubsubs[0].CreateMulticastGroup("portugal T")
	pubsubs[1].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[2].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[3].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 20)
	pubsubs[4].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[5].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[6].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[7].myPremiumSubscribe("portugal T", pubsubs[0].serverAddr, "portugal T", 10)
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is great!", "portugal T")
	time.Sleep(time.Second)

	pubsubs[3].terminateService()
	pubsubs[0].myPremiumPublish("portugal T", "Portugal is really great!", "portugal T")
	time.Sleep(time.Second)
}
