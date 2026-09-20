package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/raydatray/kq/bench/internal/events"
	"github.com/raydatray/kq/bench/internal/roles"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "kq-load: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, eventOutput *os.File) (err error) {
	if len(args) < 1 {
		return fmt.Errorf("usage: kq-load <producer|worker|mover|observer> --config <path> [--index N]")
	}
	role := events.Role(args[0])

	flags := flag.NewFlagSet(string(role), flag.ContinueOnError)
	configPath := flags.String("config", "", "resolved run configuration path")
	index := flags.Int("index", 0, "zero-based process index within the role")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("missing --config path")
	}

	config, err := roles.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	config.ProcessIndex = *index

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	output := events.NewWriter(eventOutput)
	defer func() {
		err = errors.Join(err, output.Close())
	}()

	switch role {
	case events.RoleProducer:
		return roles.RunProducer(ctx, config, output)
	case events.RoleWorker, events.RoleMover, events.RoleObserver:
		return fmt.Errorf("role %q not implemented yet", role)
	default:
		return fmt.Errorf("unknown role %q", role)
	}
}
