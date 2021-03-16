package contentpubsub

import (
	"context"
	"testing"
	"time"
)

// TestPubSubServerComms only wants to assure that pubsub servers can communicate
// TODO >> Ongoing
func TestPubSubServerComms(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)

	connect(t, ctx, dhts[0], dhts[1])

	var pubsubs [2]*PubSub

	for _, dht := range dhts {
		pubsubs[0] = NewPubSub(dht)
	}

}
