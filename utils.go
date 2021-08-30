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
		if aux[4][0] == 2 {
			return fmt.Sprintf("%s:%d%s", aux[2], 4, aux[4][1:])
		} else {
			return fmt.Sprintf("%s:%d%s", aux[2], 2, aux[4][1:])
		}

	} else {
		aux := strings.Split(addr.String(), "/")
		dialAddr := aux[2] + ":4" + aux[4][1:]

		return dialAddr
	}
}
