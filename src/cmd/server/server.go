package server

import (
	"github.com/jabardigitalservice/office-elastic-broker-service/config"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
)

// Start the server
func Start(cfg config.Config) error {
	serverErrors := make(chan error, 1)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		return errors.Wrap(err, "server error")

	case sig := <-shutdown:
		log.Fatalf("start shutdown: %v", sig)
	}

	return nil
}
