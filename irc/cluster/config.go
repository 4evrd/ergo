// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"time"
)

var validNodeName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Config is the `cluster` section of the ergo config.
type Config struct {
	Enabled bool
	// unique, stable name of this node within the cluster
	NodeName string `yaml:"node-name"`

	// if set, connect to this external NATS server/cluster instead of running
	// an embedded one, e.g. "nats://user:pass@nats1:4222,nats://nats2:4222"
	URL string
	// optional credentials file for the external NATS server
	CredsFile string `yaml:"creds-file"`

	Embedded EmbeddedConfig

	// number of JetStream replicas for cluster streams (1, 3 or 5)
	Replicas int

	History HistoryConfig

	// XXX this is copied from elsewhere in the config:
	HistoryExpireTime time.Duration `yaml:"-"`
}

// EmbeddedConfig configures the NATS server that runs inside each ergo node.
type EmbeddedConfig struct {
	// directory for JetStream data
	StoreDir string `yaml:"store-dir"`
	// all nodes in a cluster must use the same cluster name
	ClusterName string `yaml:"cluster-name"`
	// address to listen on for connections from other nodes, e.g. "0.0.0.0:6222";
	// empty disables clustering (single-node mode)
	Listen string
	// addresses other nodes listen on, e.g. "nats-route://node2.example.com:6222"
	Routes []string
	// shared credentials that nodes use to authenticate to each other
	RouteUsername string `yaml:"route-username"`
	RoutePassword string `yaml:"route-password"`
	// optional address for external NATS clients (monitoring, tooling);
	// empty means in-process connections only
	ClientListen string `yaml:"client-listen"`
	// credentials for external NATS clients; random if unset
	ClientUsername string `yaml:"client-username"`
	ClientPassword string `yaml:"client-password"`
}

type HistoryConfig struct {
	// replicate persistent history between nodes
	Enabled bool
	// how long the cluster stream retains history events, which bounds how far
	// back a new or long-offline node can catch up. defaults to the history
	// expire-time (0 means forever)
	StreamMaxAge time.Duration `yaml:"stream-max-age"`
}

// Equal reports whether two configs are identical.
func (c *Config) Equal(o *Config) bool {
	return c.Enabled == o.Enabled && c.NodeName == o.NodeName && c.URL == o.URL &&
		c.CredsFile == o.CredsFile && c.Replicas == o.Replicas && c.History == o.History &&
		c.HistoryExpireTime == o.HistoryExpireTime &&
		c.Embedded.StoreDir == o.Embedded.StoreDir && c.Embedded.ClusterName == o.Embedded.ClusterName &&
		c.Embedded.Listen == o.Embedded.Listen && slices.Equal(c.Embedded.Routes, o.Embedded.Routes) &&
		c.Embedded.RouteUsername == o.Embedded.RouteUsername &&
		c.Embedded.RoutePassword == o.Embedded.RoutePassword &&
		c.Embedded.ClientListen == o.Embedded.ClientListen &&
		c.Embedded.ClientUsername == o.Embedded.ClientUsername &&
		c.Embedded.ClientPassword == o.Embedded.ClientPassword
}

// Postprocess validates the config and fills in defaults.
func (c *Config) Postprocess() error {
	if !c.Enabled {
		return nil
	}
	if !validNodeName.MatchString(c.NodeName) {
		return fmt.Errorf("cluster.node-name must be 1-64 characters of A-Z, a-z, 0-9, _ or -")
	}
	switch c.Replicas {
	case 0:
		c.Replicas = 1
	case 1, 3, 5:
	default:
		return errors.New("cluster.replicas must be 1, 3 or 5")
	}
	if c.URL == "" {
		e := &c.Embedded
		if e.StoreDir == "" {
			e.StoreDir = "nats-data"
		}
		if e.ClusterName == "" {
			e.ClusterName = "ergo"
		}
		if e.Listen != "" {
			if _, _, err := net.SplitHostPort(e.Listen); err != nil {
				return fmt.Errorf("invalid cluster.embedded.listen: %w", err)
			}
			if (e.RouteUsername == "") != (e.RoutePassword == "") {
				return errors.New("cluster.embedded.route-username and route-password must be set together")
			}
		} else if len(e.Routes) != 0 {
			return errors.New("cluster.embedded.routes requires cluster.embedded.listen")
		} else if c.Replicas > 1 {
			return errors.New("cluster.replicas > 1 requires a clustered embedded server (cluster.embedded.listen) or an external NATS cluster")
		}
		for _, r := range e.Routes {
			if _, err := url.Parse(r); err != nil {
				return fmt.Errorf("invalid cluster.embedded.routes entry %q: %w", r, err)
			}
		}
		if (e.ClientUsername == "") != (e.ClientPassword == "") {
			return errors.New("cluster.embedded.client-username and client-password must be set together")
		}
		if e.ClientListen != "" {
			if _, _, err := net.SplitHostPort(e.ClientListen); err != nil {
				return fmt.Errorf("invalid cluster.embedded.client-listen: %w", err)
			}
		}
	}
	if c.History.StreamMaxAge == 0 {
		c.History.StreamMaxAge = c.HistoryExpireTime
	}
	return nil
}
