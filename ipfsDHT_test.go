package contentpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

// TestIpfsDHTSimpleInitialization only attempts to show DHT operation
func TestIpfsDHTSimpleInitialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 10)

	connect(t, ctx, dhts[8], dhts[9])
	connect(t, ctx, dhts[7], dhts[8])
	connect(t, ctx, dhts[7], dhts[9])

	connect(t, ctx, dhts[5], dhts[6])
	connect(t, ctx, dhts[4], dhts[5])
	connect(t, ctx, dhts[6], dhts[4])

	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	connect(t, ctx, dhts[2], dhts[0])
	connect(t, ctx, dhts[5], dhts[0])
	connect(t, ctx, dhts[8], dhts[0])

	if dhts[0].RoutingTable().Size() != 3 ||
		dhts[1].RoutingTable().Size() != 2 ||
		dhts[2].RoutingTable().Size() != 3 ||
		dhts[3].RoutingTable().Size() != 2 ||
		dhts[4].RoutingTable().Size() != 2 ||
		dhts[5].RoutingTable().Size() != 3 {

		t.Fatal("Failed to initialize dhts")
	}
}

// TestIpfsDHTAddrs was an attempt to understand how NearestPeer works and how
// are the peers addresses saved in the DHT >> (ip4/ip6)/(address)/(tcp/udp)/(port)
func TestIpfsDHTAddrs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])

	attr := "portugal"
	attrID := peer.ID(kb.ConvertKey(attr))

	peer := dhts[0].RoutingTable().NearestPeer(kb.ID(attrID))
	peerAddr := dhts[0].FindLocal(peer).Addrs[0]

	if peerAddr == nil {
		t.Fatal("No address")
	}
}

// TestSearchSelf was an attempt to understand which peers
// are returned once we search for the self node on the
// routing table to understand if we can use it on a
// function that returns the potencial backups
func TestSearchSelf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 10)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])
	connect(t, ctx, dhts[0], dhts[8])
	connect(t, ctx, dhts[0], dhts[9])

	backups := dhts[0].RoutingTable().NearestPeers(kb.ConvertPeerID(dhts[0].PeerID()), 3)

	if len(backups) != 3 {
		t.Fatal("Error getting backups")
	}
}

// TestDHTRefreshing
// Conclusions >> this function should not be used how it is being used
// because we should not be refreshing the table instantly because
// in order to receive confirmation that a node is down we need to wait
// for a long time
func TestDHTRefreshing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 10)

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])
	connect(t, ctx, dhts[0], dhts[4])
	connect(t, ctx, dhts[0], dhts[5])
	connect(t, ctx, dhts[0], dhts[6])
	connect(t, ctx, dhts[0], dhts[7])
	connect(t, ctx, dhts[0], dhts[8])
	connect(t, ctx, dhts[0], dhts[9])

	if len(dhts[0].RoutingTable().GetPeerInfos()) != 9 {
		t.Fatal("Wrong number of peers")
	}

	dhts[1].Close()
	time.Sleep(time.Second)

	dhts[0].RefreshRoutingTable()
}

func TestPeerStuff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)

	connect(t, ctx, dhts[0], dhts[1])

	a := peer.Encode(dhts[1].PeerID())

	b, _ := peer.Decode(a)

	addr := dhts[0].FindLocal(b).Addrs[0]

	if addr == nil {
		t.Fatal("bolas")
	}
}
