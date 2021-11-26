package contentpubsub

import (
	"fmt"
	"strings"

	cidRepo "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
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

func attributeCid(attr string) string {

	h, _ := mh.Sum([]byte(attr), mh.SHA3, 4)
	res := cidRepo.NewCidV1(7, h)

	return res.KeyString()
}
