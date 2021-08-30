package contentpubsub

import (
	"fmt"
	"strings"

	"github.com/multiformats/go-multiaddr"
)

const TestgroundReady = true

func addrForPubSubServer(addr multiaddr.Multiaddr) string {

	if TestgroundReady {
		aux := strings.Split(addr.String(), "/")
		return fmt.Sprintf("%s:%d%s", aux[2], 1, aux[4][1:])
	} else {
		aux := strings.Split(addr.String(), "/")
		dialAddr := aux[2] + ":4" + aux[4][1:]

		return dialAddr
	}
}
