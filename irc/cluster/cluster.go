// Copyright (c) 2026 4evrd contributors
// released under the MIT license

// Package cluster connects ergo nodes to each other over NATS. By default each
// node runs an embedded NATS server and the embedded servers form a cluster
// among themselves (routes), so no external infrastructure is needed; an
// external NATS deployment can be used instead by setting cluster.url.
package cluster

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
)

// how long to wait for the embedded NATS server to start (a var for tests)
var startupTimeout = 30 * time.Second

// Cluster is this node's connection to the cluster.
type Cluster struct {
	config Config
	logger *logger.Manager

	ns *natsserver.Server // nil when using external NATS

	clientUser, clientPass string
	nc                     *nats.Conn
	js                     jetstream.JetStream
}

// NodeName returns the name of this node.
func (c *Cluster) NodeName() string {
	return c.config.NodeName
}

// JetStream returns the JetStream context for this node.
func (c *Cluster) JetStream() jetstream.JetStream {
	return c.js
}

// Conn returns the underlying NATS connection.
func (c *Cluster) Conn() *nats.Conn {
	return c.nc
}

// Start starts the embedded NATS server (if configured) and connects to it.
func Start(logger *logger.Manager, config Config) (c *Cluster, err error) {
	c = &Cluster{config: config, logger: logger}

	connOpts := []nats.Option{
		nats.Name("ergo-" + config.NodeName),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warning("cluster", "disconnected from NATS:", err.Error())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("cluster", "reconnected to NATS at", nc.ConnectedUrlRedacted())
		}),
	}

	url := config.URL
	if url == "" {
		if err = c.startEmbedded(); err != nil {
			return nil, err
		}
		connOpts = append(connOpts, nats.InProcessServer(c.ns), nats.UserInfo(c.clientUser, c.clientPass))
	} else if config.CredsFile != "" {
		connOpts = append(connOpts, nats.UserCredentials(config.CredsFile))
	}

	c.nc, err = nats.Connect(url, connOpts...)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("could not connect to NATS: %w", err)
	}
	c.js, err = jetstream.New(c.nc)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("could not initialize JetStream: %w", err)
	}
	logger.Info("cluster", fmt.Sprintf("node %s connected to NATS", config.NodeName))
	return c, nil
}

func splitHostPort(addr string) (host string, port int, err error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(portStr)
	return
}

func (c *Cluster) startEmbedded() error {
	e := c.config.Embedded
	// clients (including our own in-process connection) must authenticate;
	// without configured credentials, generate random ones so nothing else
	// on the machine can connect and inject events
	c.clientUser, c.clientPass = e.ClientUsername, e.ClientPassword
	if c.clientUser == "" {
		c.clientUser, c.clientPass = "ergo", utils.GenerateSecretToken()
	}
	opts := &natsserver.Options{
		ServerName: c.config.NodeName,
		JetStream:  true,
		StoreDir:   e.StoreDir,
		NoSigs:     true,
		Username:   c.clientUser,
		Password:   c.clientPass,
		// nats-server can't route with DontListen (routing waits on the client
		// accept loop), so default to a random loopback port
		Host: "127.0.0.1",
		Port: natsserver.RANDOM_PORT,
	}
	if e.ClientListen != "" {
		host, port, err := splitHostPort(e.ClientListen)
		if err != nil {
			return err
		}
		opts.Host, opts.Port = host, port
	}
	if e.Listen != "" {
		host, port, err := splitHostPort(e.Listen)
		if err != nil {
			return err
		}
		opts.Cluster = natsserver.ClusterOpts{
			Name:     e.ClusterName,
			Host:     host,
			Port:     port,
			Username: e.RouteUsername,
			Password: e.RoutePassword,
		}
		for _, r := range e.Routes {
			u, err := url.Parse(r)
			if err != nil {
				return err
			}
			if u.User == nil && e.RouteUsername != "" {
				u.User = url.UserPassword(e.RouteUsername, e.RoutePassword)
			}
			opts.Routes = append(opts.Routes, u)
		}
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("could not create embedded NATS server: %w", err)
	}
	log := &natsLogger{logger: c.logger}
	ns.SetLoggerV2(log, false, false, false)
	go ns.Start()
	deadline := time.Now().Add(startupTimeout)
	for !ns.ReadyForConnections(100 * time.Millisecond) {
		if fatal := log.fatalError(); fatal != "" {
			ns.Shutdown()
			return fmt.Errorf("embedded NATS server failed to start: %s", fatal)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("embedded NATS server did not start within %v", startupTimeout)
		}
	}
	// JetStream can fail after the listeners are up
	if fatal := log.fatalError(); fatal != "" {
		ns.Shutdown()
		return fmt.Errorf("embedded NATS server failed to start: %s", fatal)
	}
	c.ns = ns
	if e.Listen != "" {
		c.logger.Info("cluster", "embedded NATS listening for other nodes on", e.Listen)
	}
	return nil
}

// Close disconnects from NATS and shuts down the embedded server.
func (c *Cluster) Close() {
	if c.nc != nil {
		c.nc.FlushTimeout(2 * time.Second)
		c.nc.Close()
	}
	if c.ns != nil {
		c.ns.Shutdown()
		c.ns.WaitForShutdown()
	}
}

type natsLogger struct {
	logger *logger.Manager

	mu    sync.Mutex
	fatal string
}

func (l *natsLogger) fatalError() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fatal
}

func (l *natsLogger) Noticef(format string, v ...any) {
	l.logger.Debug("nats", fmt.Sprintf(format, v...))
}

func (l *natsLogger) Warnf(format string, v ...any) {
	l.logger.Warning("nats", fmt.Sprintf(format, v...))
}

// Fatalf records the error so startup can fail; nats-server expects Fatalf
// not to return, but the embedded server must not exit the ergo process
func (l *natsLogger) Fatalf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Error("nats", "fatal:", msg)
	l.mu.Lock()
	if l.fatal == "" {
		l.fatal = msg
	}
	l.mu.Unlock()
}

func (l *natsLogger) Errorf(format string, v ...any) {
	l.logger.Error("nats", fmt.Sprintf(format, v...))
}

func (l *natsLogger) Debugf(format string, v ...any) {
	l.logger.Debug("nats", fmt.Sprintf(format, v...))
}

func (l *natsLogger) Tracef(format string, v ...any) {}
