package contentpubsub

import (
	"strings"

	"github.com/multiformats/go-multiaddr"
)

func addrForPubSubServer(addr multiaddr.Multiaddr) string {

	aux := strings.Split(addr.String(), "/")
	dialAddr := aux[2] + ":4" + aux[4][1:]

	return dialAddr
}
