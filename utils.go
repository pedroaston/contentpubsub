package contentpubsub

import (
	"fmt"
	"strings"

	cid "github.com/ipfs/go-cid"
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

func attributeCID(attr string) string {
	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, _ := pref.Sum([]byte(attr))

	return c.KeyString()
}
