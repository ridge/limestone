package tnet

import (
	"context"
	"net"
	"strings"

	"github.com/ridge/must/v2"
	"time"
)

var lc = net.ListenConfig{
	KeepAlive: 3 * time.Minute,
}

// Listen installs a listener on the specified address.
//
// If the address string starts with "tcp:", the rest is interpreted as
// [address]:port on which to open a TCP listening socket. TCP keep-alive is
// enabled in this case.
//
// If the address string starts with "unix:", the rest is interpreted the path
// to a UNIX domain socket to listen on.
//
// If neither prefix is present, "tcp:" is assumed.
func Listen(address string) (net.Listener, error) {
	network := "tcp"
	proto, rest, ok := strings.Cut(address, ":")
	if ok {
		switch proto {
		case "unix":
			network = "unix"
			address = rest
		case "tcp":
			address = rest
		}
	}
	return lc.Listen(context.Background(), network, address)
}

// ListenOnRandomPort selects a random local TCP port and installs a listener on
// it with TCP keep-alive enabled
func ListenOnRandomPort() net.Listener {
	return must.OK1(Listen("localhost:"))
}
