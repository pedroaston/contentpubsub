package contentpubsub

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/multiformats/go-multiaddr"
)

const InstrumentedVersion = true

func addrForPubSubServer(addr multiaddr.Multiaddr) string {

	if InstrumentedVersion {
		aux := strings.Split(addr.String(), "/")
		i, _ := strconv.Atoi(aux[4])
		lastDigit := (i + 1) % 10
		dialAddr := fmt.Sprintf("%s:%s%d", aux[2], aux[4][:len(aux[4])-1], lastDigit)

		return dialAddr
	} else {
		aux := strings.Split(addr.String(), "/")
		dialAddr := aux[2] + ":4" + aux[4][1:]

		return dialAddr
	}
}
