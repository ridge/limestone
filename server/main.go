package server

import (
	"context"
	"fmt"
	"net"

	"github.com/gorilla/mux"
	"github.com/ridge/limestone/client"
	"github.com/ridge/limestone/kafka"
	"github.com/ridge/limestone/run"
	"github.com/ridge/limestone/thttp"
	"github.com/ridge/limestone/tnet"
	"github.com/ridge/limestone/wire"
	"github.com/ridge/parallel"
	"github.com/spf13/pflag"
)

// Config contains the server parameters
type Config struct {
	Listener        net.Listener
	Kafka           kafka.Client
	HotStartStorage string
}

// Main handles the command line and runs the server
func Main(args []string) {
	run.Server(func(ctx context.Context) error {
		var addr, hotStartStorage, kafkaURL string
		pflag.StringVar(&addr, "addr", ":10007", "address to listen on")
		pflag.StringVar(&hotStartStorage, "hot-start", "", "Google Cloud Storage URL prefix (gs://...) for hot start data")
		pflag.StringVar(&kafkaURL, "kafka-url", "", "Kafka URL")
		_ = pflag.CommandLine.Parse(args[1:])

		kafka, err := kafka.FromURI(kafkaURL)
		if err != nil {
			return err
		}

		listener, err := tnet.Listen(addr)
		if err != nil {
			return err
		}

		return Run(ctx, Config{
			Listener:        listener,
			Kafka:           kafka,
			HotStartStorage: hotStartStorage,
		})
	})
}

// Run runs the server
func Run(ctx context.Context, config Config) error {
	upstream := client.NewKafkaClient(config.Kafka)
	manifest, err := upstream.RetrieveManifest(ctx, true)
	if err != nil {
		return err
	}
	server := server{
		upstream: upstream,
		master:   upstream.Connect(manifest.Version, wire.Beginning, nil, false),
		version:  manifest.Version,
	}

	if config.HotStartStorage != "" {
		if err := server.pullHotStart(ctx, fmt.Sprintf("%s%d", config.HotStartStorage, manifest.Version)); err != nil {
			return err
		}
	}

	router := mux.NewRouter()
	router.HandleFunc("/pull", server.pull)
	router.HandleFunc("/push", server.push)
	httpServer := thttp.NewServer(config.Listener, thttp.StandardMiddleware(router))

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("server", parallel.Fail, server.Run)
		spawn("http", parallel.Fail, httpServer.Run)
		return nil
	})
}

type server struct {
	upstream client.KafkaClient
	master   client.Connection

	version int

	hotStart    map[string][][]byte // kind -> messages
	hotStartPos wire.Position
}

func (s server) Run(ctx context.Context) error {
	// This connection is only used to submit transactions, so we won't read from it
	return s.master.Run(ctx, make(chan *wire.IncomingTransaction))
}
