package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"github.com/go-pkgz/lgr"
	"github.com/jessevdk/go-flags"
)

var Options struct {
	Port int `long:"port" short:"p" env:"PORT" description:"redis port" default:"6379"`
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

	// Start the server
	bind := net.JoinHostPort("0.0.0.0", strconv.Itoa(Options.Port))
	s := NewServer(bind)
	log.Printf("[INFO] Starting server on: %s", bind)
	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("[ERROR] error starting server: %e", err)
		os.Exit(1)
	}
}
