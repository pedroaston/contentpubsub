package contentpubsub

import (
	"fmt"
	"strings"

	"github.com/multiformats/go-multiaddr"
)

func addrForPubSubServer(addrs []multiaddr.Multiaddr, addrOption bool) string {

	for _, addr := range addrs {
		aux := strings.Split(addr.String(), "/")
		if aux[3] == "tcp" {
			if addrOption {
				dialAddr := fmt.Sprintf("%s:%s", aux[2], "31313")
				return dialAddr
			} else {
				dialAddr := aux[2] + ":3" + aux[4][1:]
				return dialAddr
			}
		}
	}

	return ""
}
