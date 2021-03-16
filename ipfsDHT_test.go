package contentpubsub

import (
	"context"
	"testing"
	"time"
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
