package tws

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
	"time"
)

func tuneTCP(conn net.Conn, config Config) error {
	if config.TCPTimeout != 0 {
		if err := setTCPOption(conn, unix.TCP_USER_TIMEOUT, int(config.TCPTimeout/time.Millisecond)); err != nil {
			return fmt.Errorf("failed to tune TCP socket: %w", err)
		}
	}

	return nil
}
