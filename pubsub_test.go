package contentpubsub

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/pedroaston/contentpubsub/pb"
	"google.golang.org/grpc"
)

// TestPubSubServerComms only wants to assure that pubsub servers can communicate
// by initializing both and subscribing to events about portugal
func TestPubSubServerComms(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)
	connect(t, ctx, dhts[0], dhts[1])

	var pubsubs [2]*PubSub
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht)

		addr := dhts[i].Host().Addrs()[0]
		aux := strings.Split(addr.String(), "/")
		dialAddr := aux[2] + ":4" + aux[4][1:]

		lis, err := net.Listen("tcp", dialAddr)
		if err != nil {
			t.Fatal(err)
		}

		grpcServer := grpc.NewServer()
		pb.RegisterScoutHubServer(grpcServer, pubsubs[i])
		go grpcServer.Serve(lis)
	}

	err := pubsubs[0].MySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if pubsubs[1].currentFilterTable.routes[peer.Encode(pubsubs[0].ipfsDHT.PeerID())].filters[1][0].String() != "<portugal> " {
		t.Fatal("Failed Subscription")
	}
}
