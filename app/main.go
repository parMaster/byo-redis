package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/go-pkgz/lgr"
	"github.com/jessevdk/go-flags"
)

var Options struct {
	Port      int    `long:"port" short:"p" env:"PORT" description:"redis port" default:"6379"`
	ReplicaOf string `long:"replicaof" short:"r" env:"REPLICA_OF" description:"master connection credentials: <ip> <port>" default:""`
}

func main() {
	// Parse flags
	if _, err := flags.Parse(&Options); err != nil {
		os.Exit(1)
	}

	// Logger setup
	logOpts := []lgr.Option{
		lgr.LevelBraces,
		lgr.StackTraceOnError,
	}
	logOpts = append(logOpts, lgr.Debug)
	lgr.SetupStdLogger(logOpts...)

	bind := net.JoinHostPort("0.0.0.0", strconv.Itoa(Options.Port))
	s := NewServer(bind)

	// Start the server
	if Options.ReplicaOf != "" {
		replicaOf := strings.Split(Options.ReplicaOf, " ")
		if len(replicaOf) != 2 {
			log.Fatalf("[ERROR] invalid replicaof option: %s, use space separated <ip> <port>", Options.ReplicaOf)
			os.Exit(1)
		}
		log.Printf("[INFO] Starting as replica of %s:%s", replicaOf[0], replicaOf[1])
		s.AsSlaveOf(net.JoinHostPort(replicaOf[0], replicaOf[1]))
	}

	log.Printf("[INFO] Starting server on: %s", bind)
	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("[ERROR] error starting server: %e", err)
		os.Exit(1)
	}
}
