package contentpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	ma "github.com/multiformats/go-multiaddr"

	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// The following functions and types were extracted from
// go-libp2p-kad-dht to test the dht operations of this work
// and other were created by me to assist the tests
type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

var testPrefix = dht.ProtocolPrefix("/test")

func setupDHT(ctx context.Context, t *testing.T, client bool, options ...dht.Option) *dht.IpfsDHT {
	baseOpts := []dht.Option{
		testPrefix,
		dht.NamespacedValidator("v", blankValidator{}),
		dht.DisableAutoRefresh(),
	}

	if client {
		baseOpts = append(baseOpts, dht.Mode(dht.ModeClient))
	} else {
		baseOpts = append(baseOpts, dht.Mode(dht.ModeServer))
	}

	d, err := dht.New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, options...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func setupDHTS(t *testing.T, ctx context.Context, n int, options ...dht.Option) []*dht.IpfsDHT {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*dht.IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false, options...)
		peers[i] = dhts[i].PeerID()
		addrs[i] = dhts[i].Host().Addrs()[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()

	idB := b.Host().ID()
	addrB := b.Host().Peerstore().Addrs(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}

	a.Host().Peerstore().AddAddrs(idB, addrB, peerstore.TempAddrTTL)
	pi := peer.AddrInfo{ID: idB}
	if err := a.Host().Connect(ctx, pi); err != nil {
		t.Fatal(err)
	}
}

func wait(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()

	for a.RoutingTable().Find(b.Host().ID()) == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()
	connectNoSync(t, ctx, a, b)
	wait(t, ctx, a, b)
	wait(t, ctx, b, a)
}

/* Not being used now
func bootstrapDhts(t *testing.T, ctx context.Context, dhts []*dht.IpfsDHT) {
	connectionMap := make(map[int][]int)
	totalDHT := len(dhts) - 1
	var random int
	var connected bool

	for i, dht := range dhts {
		for len(connectionMap[i]) < totalDHT/4 {
			random = rand.Intn(totalDHT)
			connected = false

			for _, value := range connectionMap[i] {
				if value == random || i == random {
					connected = true
					break
				}
			}

			if !connected {
				connect(t, ctx, dht, dhts[random])
				connectionMap[i] = append(connectionMap[i], random)
				connectionMap[random] = append(connectionMap[i], i)
			}
		}
	}
}
*/
