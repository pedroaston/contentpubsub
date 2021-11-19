package contentpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

// TestSubAndUnsub just subscribes to certain
// events and then unsubscribes to them
// Test composition: 3 nodes
// >> 1 Subscriber that subscribes and unsubscribes
// >> 2 nodes that saves the other's subscription
func TestSubAndUnsub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 4)
	var pubsubs [4]*PubSub
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for _, pubsub := range pubsubs {
			pubsub.TerminateService()
		}
	}()

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[2]])

	// Initialize pub-subs
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	// The peer at the edge of the chain subscribes
	err := pubsubs[helper[2]].MySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if len(pubsubs[helper[2]].myFilters.filters[1]) != 1 {
		t.Fatal("Error Subscribing!")
	}

	// Wait for subscriptions to be disseminated
	time.Sleep(time.Second)

	// Confirm that all intermidiate nodes plus the rendezvous have a filter
	if len(pubsubs[helper[1]].currentFilterTable.routes[peer.Encode(pubsubs[helper[2]].ipfsDHT.PeerID())].filters[1]) != 1 ||
		len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[1]].ipfsDHT.PeerID())].filters[1]) != 1 {

		t.Fatal("Failed Unsubscribing!")
	}

	// Unsubscribing operation
	pubsubs[helper[2]].MyUnsubscribe("portugal T")
	if len(pubsubs[helper[2]].myFilters.filters[1]) != 0 {
		t.Fatal("Failed Unsubscribing!")
	}
}

// TestPublish simply subscribes to a event and then publishes it
// Test composition: 3 nodes
// >> 1 Publisher that publishes a event
// >> 1 Subscribers that subscribe to a event
// >> 1 passive node acting as rendezvous of the attribute portugal
func TestPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 4)
	var pubsubs [4]*PubSub
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for _, pubsub := range pubsubs {
			pubsub.TerminateService()
		}
	}()

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[2]])

	// Initialize pub-subs
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	// The peer at the edge of the chain subscribes
	pubsubs[helper[2]].MySubscribe("portugal T")
	time.Sleep(time.Second)

	// The peer at the middle of the chain publishes
	pubsubs[helper[1]].MyPublish("Portugal is beautifull!", "portugal T")
	time.Sleep(time.Second)

	// Confirm if subscriber received the event
	var expected []string
	expected = append(expected, "Portugal is beautifull!")
	miss0, _ := pubsubs[helper[2]].ReturnCorrectnessStats(expected)
	if miss0 != 0 {
		t.Fatal("event not received by subscriber")
	}
}

// TestFaultTolerance just tries to show the system
// working with failures in different situations and
// maitaining its functioning
// Test composition: 4 nodes
// >> 1 Publisher publishes a kind of event
// >> 2 Subscribers subscribing a kind of event
// >> 1 Passive node that just diffuse the
// subscriptions that will fail between the
// subscription stage and the event publishing
func TestFaultTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var pubsubs [5]*PubSub
	dhts := setupDHTS(t, ctx, 5)

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[4].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[2]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[3]])

	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for i, pubsub := range pubsubs {
			if i != helper[1] {
				pubsub.TerminateService()
			}
		}
	}()

	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	pubsubs[helper[3]].MySubscribe("portugal T")
	pubsubs[helper[2]].MySubscribe("portugal T")
	time.Sleep(time.Second)

	pubsubs[helper[1]].TerminateService()
	time.Sleep(time.Second)

	pubsubs[helper[0]].MyPublish("valmitão tem as melhores marolas do mundo!", "portugal T")
	time.Sleep(5 * time.Second)

	// Confirm if subscribers received the event
	var expected []string
	expected = append(expected, "valmitão tem as melhores marolas do mundo!")
	miss0, _ := pubsubs[helper[2]].ReturnCorrectnessStats(expected)
	miss1, _ := pubsubs[helper[3]].ReturnCorrectnessStats(expected)
	if miss0 != 0 || miss1 != 0 {
		t.Fatal("event not received by at least one subscriber")
	}
}

// TestRedirectMechanism shows that a event can jump several hops on
// the network if those intermidiate nodes don't lead to more subscribers
// Test composition: 4 nodes
// >> 1 Subscriber subscribes at the bottom of the dissemination chain
// >> 1 Publisher publishing a event next to the dissemination chain
// >> 2 passive nodes
func TestRedirectMechanism(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 5)
	var pubsubs [5]*PubSub
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for _, pubsub := range pubsubs {
			pubsub.TerminateService()
		}
	}()

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[4].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[1]], dhts[helper[0]])
	connect(t, ctx, dhts[helper[0]], dhts[helper[2]])
	connect(t, ctx, dhts[helper[2]], dhts[helper[3]])

	// Initialize pub-subs
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	// The peer at the edge of the chain subscribes
	pubsubs[helper[3]].MySubscribe("portugal T")
	time.Sleep(time.Second)

	// The peer at the middle of the chain publishes
	pubsubs[helper[1]].MyPublish("Portugal is beautifull!", "portugal T")
	time.Sleep(time.Second)

	// Confirm if the event was received by the subscriber and if it took the shortcut
	var expected []string
	expected = append(expected, "Portugal is beautifull!")
	miss0, _ := pubsubs[helper[3]].ReturnCorrectnessStats(expected)
	op0 := pubsubs[helper[2]].ReturnOpStats("Notify")
	if miss0 != 0 {
		t.Fatal("event not received by subscriber")
	} else if op0 != 0 {
		t.Fatal("event failed to jump")
	}
}

// TestRefreshing performs two subscriptions and right after unsubs
// one of those. This way after the refreshing of the filter tables
// only one of the subscriptions will remain on the network
// Test composition: 3 nodes
// >> 2 Subscriber that subscribes and unsubscribes
// >> 1 node acting has rendezvous
func TestRefreshing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 4)
	var pubsubs [4]*PubSub
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for _, pubsub := range pubsubs {
			pubsub.TerminateService()
		}
	}()

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[0]], dhts[helper[2]])

	// Initialize pub-subs
	cfg := DefaultConfig("PT", 10)
	cfg.SubRefreshRateMin = 5 * time.Second
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, cfg)
		pubsubs[i].SetHasOldPeer()
	}

	// Two peers subscribe and one imediatly unsubs
	pubsubs[helper[1]].MySubscribe("portugal T")
	pubsubs[helper[2]].MySubscribe("portugal T")
	pubsubs[helper[2]].MyUnsubscribe("portugal T")

	// Wait for subscriptions to be disseminated
	time.Sleep(time.Second)

	// Confirm that the rendezvous has the filters
	if len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[2]].ipfsDHT.PeerID())].filters[1]) != 1 ||
		len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[1]].ipfsDHT.PeerID())].filters[1]) != 1 {

		t.Fatal("Failed Subscribing!")
	}

	// Wait a complete refresh cycle
	time.Sleep(23 * time.Second)

	// Confirm that the rendezvous has the filters
	if len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[2]].ipfsDHT.PeerID())].filters[1]) != 0 ||
		len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[1]].ipfsDHT.PeerID())].filters[1]) != 1 {

		t.Fatal("Incorrect state after refreshing!")
	}
}
