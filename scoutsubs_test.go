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
// >> 3 Subscribers subscribing a kind of event
// >> 3 Passive node that just diffuse the
// subscriptions that will fail between the
// subscription stage and the event publishing
func TestFaultTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var pubsubs [8]*PubSub
	dhts := setupDHTS(t, ctx, 8)

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[4].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[5].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[6].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[7].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[0]], dhts[helper[2]])
	connect(t, ctx, dhts[helper[2]], dhts[helper[6]])
	connect(t, ctx, dhts[helper[2]], dhts[helper[3]])
	connect(t, ctx, dhts[helper[3]], dhts[helper[5]])
	connect(t, ctx, dhts[helper[3]], dhts[helper[4]])

	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for i, pubsub := range pubsubs {
			if i != helper[2] && i != helper[3] {
				pubsub.TerminateService()
			}
		}
	}()

	cfg := DefaultConfig("PT", 10)
	cfg.FaultToleranceFactor = 3
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, cfg)
		pubsubs[i].SetHasOldPeer()
	}

	pubsubs[helper[4]].MySubscribe("portugal T")
	pubsubs[helper[5]].MySubscribe("portugal T")
	pubsubs[helper[6]].MySubscribe("portugal T")
	time.Sleep(time.Second)

	pubsubs[helper[2]].TerminateService()
	pubsubs[helper[3]].TerminateService()
	time.Sleep(time.Second)

	pubsubs[helper[1]].MyPublish("valmitão tem as melhores marolas do mundo!", "portugal T")
	time.Sleep(10 * time.Second)

	// Confirm if subscribers received the event
	var expected []string
	expected = append(expected, "valmitão tem as melhores marolas do mundo!")
	miss4, _ := pubsubs[helper[4]].ReturnCorrectnessStats(expected)
	miss5, _ := pubsubs[helper[5]].ReturnCorrectnessStats(expected)
	miss6, _ := pubsubs[helper[6]].ReturnCorrectnessStats(expected)
	if miss4 != 0 || miss5 != 0 || miss6 != 0 {
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

// TestAcknowledgeChain confirms if the entire acknowledge
// and tracking mechanisms were successfully completed
// after a successful event delivery
// Test composition: 6 nodes
// >> 1 Publisher that publishes a event
// >> 3 Subscribers that subscribe to a event
// >> 2 passive nodes one acting as rendezvous of
//      the attribute portugal and another as an
//      intermidiate one
func TestAcknowledgeChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 7)
	var pubsubs [7]*PubSub
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
	connect(t, ctx, dhts[0], dhts[5])
	connect(t, ctx, dhts[0], dhts[6])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[4].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[5].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[6].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[2]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[3]])
	connect(t, ctx, dhts[helper[3]], dhts[helper[4]])
	connect(t, ctx, dhts[helper[3]], dhts[helper[5]])

	// Initialize pub-subs
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	// The peer at the edge of the chain subscribes
	pubsubs[helper[2]].MySubscribe("portugal T")
	pubsubs[helper[4]].MySubscribe("portugal T")
	pubsubs[helper[5]].MySubscribe("portugal T")
	time.Sleep(time.Second)

	// The peer at the middle of the chain publishes
	pubsubs[helper[1]].MyPublish("Portugal is beautifull!", "portugal T")
	time.Sleep(time.Second)

	// Confirm if subscriber received the event
	var expected []string
	expected = append(expected, "Portugal is beautifull!")
	miss2, _ := pubsubs[helper[2]].ReturnCorrectnessStats(expected)
	miss4, _ := pubsubs[helper[4]].ReturnCorrectnessStats(expected)
	miss5, _ := pubsubs[helper[5]].ReturnCorrectnessStats(expected)
	if miss2 != 0 || miss4 != 0 || miss5 != 0 {
		t.Fatal("event not received by subscriber")
	}

	// Confirm that publisher received confirmation
	// that the rv received the event
	if pubsubs[helper[1]].totalUnconfirmedEvents != 0 {
		t.Fatal("event not confirmed by rendezvous")
	}

	// Confirm that subscribers received confirmation
	// that the rv received the subscriptions
	if pubsubs[helper[2]].totalUnconfirmedEvents != 0 || pubsubs[helper[4]].totalUnconfirmedEvents != 0 ||
		pubsubs[helper[5]].totalUnconfirmedEvents != 0 {
		t.Fatal("subscription not confirmed by rendezvous")
	}

	// Confirm the intermidiate nodes received all acks
	for _, eLog := range pubsubs[helper[1]].myETrackers {
		if eLog.expectedAcks != eLog.receivedAcks {
			t.Fatal("unreceived acks at 1st intermidiate")
		}
	}
	for _, eLog := range pubsubs[helper[3]].myETrackers {
		if eLog.expectedAcks != eLog.receivedAcks {
			t.Fatal("unreceived acks at 2st intermidiate")
		}
	}

	// Confirm that the tracker has received all acks
	for _, eLog := range pubsubs[helper[0]].myTrackers["portugal"].eventStats {
		if eLog.expectedAcks != eLog.receivedAcks {
			t.Fatal("unreceived acks at tracker")
		}
	}
}
