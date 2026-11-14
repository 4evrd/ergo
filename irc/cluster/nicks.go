// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	nicksBucket = "ERGO_NICKS"
	nodesBucket = "ERGO_NODES"
	nickKeyPfx  = "n."

	defaultNodeTimeout = 15 * time.Second
	kvTimeout          = 5 * time.Second
)

var (
	// ErrNickInUse means another live node holds the nick.
	ErrNickInUse = errors.New("nickname is in use on another node")
	// ErrUnavailable means the cluster can't currently decide nick ownership
	// (e.g. no quorum); callers should refuse the nick rather than risk a
	// duplicate.
	ErrUnavailable = errors.New("cluster nick registry is unavailable")
)

// NickClaim is the value stored for a claimed nick.
type NickClaim struct {
	Node    string `json:"node"`
	Nick    string `json:"nick"`
	Account string `json:"account,omitempty"`
}

// Nicks is the cluster-wide registry of nicknames. Nicks are keyed by their
// skeleton (the confusables-folded form, which subsumes casefolding), so one
// claim per skeleton guarantees uniqueness across the network.
//
// Callers must serialize Claim/Release calls for the same skeleton on this
// node (see irc.clusterNicks); cross-node races are resolved by the KV
// store's compare-and-set operations.
type Nicks struct {
	c           *Cluster
	nodeTimeout time.Duration

	mu    sync.RWMutex
	nicks jetstream.KeyValue // nil until set up
	nodes jetstream.KeyValue

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StartNicks sets up the nick registry and starts this node's heartbeat.
// Setup is retried in the background; until it succeeds, Claim returns
// ErrUnavailable.
func (c *Cluster) StartNicks() *Nicks {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Nicks{c: c, nodeTimeout: c.config.Nicks.NodeTimeout, ctx: ctx, cancel: cancel}
	if n.nodeTimeout <= 0 {
		n.nodeTimeout = defaultNodeTimeout
	}
	n.wg.Add(1)
	go n.run()
	return n
}

func (n *Nicks) run() {
	defer n.wg.Done()
	for {
		err := n.setup()
		if err == nil {
			break
		}
		n.c.logger.Warning("cluster", "nick registry not ready, retrying:", err.Error())
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(setupRetryDelay):
		}
	}
	n.c.logger.Info("cluster", "nick registry active")
	n.heartbeatLoop()
}

func (n *Nicks) setup() error {
	ctx, cancel := context.WithTimeout(n.ctx, setupTimeout)
	defer cancel()
	js := n.c.js
	nicks, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      nicksBucket,
		Description: "ergo cluster-wide nickname claims",
		History:     1,
		Storage:     jetstream.FileStorage,
		Replicas:    n.c.config.Replicas,
	})
	if err != nil {
		return fmt.Errorf("could not create nick bucket: %w", err)
	}
	nodes, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      nodesBucket,
		Description: "ergo node heartbeats",
		History:     1,
		TTL:         n.nodeTimeout,
		Storage:     jetstream.FileStorage,
		Replicas:    n.c.config.Replicas,
	})
	if err != nil {
		return fmt.Errorf("could not create node bucket: %w", err)
	}
	// announce ourselves before accepting claims, so other nodes treat our
	// claims as live
	if _, err := nodes.Put(ctx, n.c.config.NodeName, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("could not register node: %w", err)
	}
	n.mu.Lock()
	n.nicks, n.nodes = nicks, nodes
	n.mu.Unlock()
	return nil
}

func (n *Nicks) heartbeatLoop() {
	ticker := time.NewTicker(n.nodeTimeout / 3)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.heartbeat()
		}
	}
}

func (n *Nicks) heartbeat() {
	_, nodes := n.buckets()
	if nodes == nil {
		n.c.logger.Debug("cluster", "skipping node heartbeat while disconnected from NATS")
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, kvTimeout)
	defer cancel()
	if _, err := nodes.Put(ctx, n.c.config.NodeName, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		n.c.logger.Warning("cluster", "could not send node heartbeat:", err.Error())
	}
}

// buckets returns the KV buckets, or nil if the registry isn't set up or the
// NATS connection is down (so callers fail fast instead of waiting out a
// timeout on every nick change during an outage)
func (n *Nicks) buckets() (nicks, nodes jetstream.KeyValue) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.c != nil && !n.c.nc.IsConnected() {
		return nil, nil
	}
	return n.nicks, n.nodes
}

// WaitReady waits up to timeout for the registry to be set up.
func (n *Nicks) WaitReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !n.Ready() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// Ready reports whether the registry is set up.
func (n *Nicks) Ready() bool {
	nicks, _ := n.buckets()
	return nicks != nil
}

// Close stops the heartbeat and deregisters this node, so other nodes can
// take over its nicks right away. Claims are left in place: another node
// takes each over when it's next requested, and this node's next start
// releases the rest.
func (n *Nicks) Close() {
	n.cancel()
	n.wg.Wait()
	if _, nodes := n.buckets(); nodes != nil {
		ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
		defer cancel()
		if err := nodes.Delete(ctx, n.c.config.NodeName); err != nil {
			n.c.logger.Warning("cluster", "could not deregister node:", err.Error())
		}
	}
}

// key encoding: KV keys allow only [-/_=.a-zA-Z0-9], so skeletons are hex-encoded
func nickKey(skeleton string) string {
	return nickKeyPfx + hex.EncodeToString([]byte(skeleton))
}

func skeletonFromKey(key string) (string, bool) {
	b, err := hex.DecodeString(strings.TrimPrefix(key, nickKeyPfx))
	return string(b), err == nil && strings.HasPrefix(key, nickKeyPfx)
}

// nodeAlive reports whether node has sent a heartbeat within the timeout.
func (n *Nicks) nodeAlive(ctx context.Context, nodes jetstream.KeyValue, node string) (bool, error) {
	_, err := nodes.Get(ctx, node)
	if err == nil {
		return true, nil
	} else if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return false, nil
	}
	return false, err
}

func unavailable(err error) error {
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// Claim claims skeleton for this node. It returns acquired=true if this call
// created (or took over) the claim, in which case the caller must Release it
// if it ends up not using the nick. If this node already holds the claim,
// it returns acquired=false and a nil error: local state decides.
func (n *Nicks) Claim(skeleton string, claim NickClaim) (acquired bool, err error) {
	nicks, nodes := n.buckets()
	if nicks == nil {
		return false, ErrUnavailable
	}
	claim.Node = n.c.config.NodeName
	data, err := json.Marshal(claim)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(n.ctx, kvTimeout)
	defer cancel()

	key := nickKey(skeleton)
	_, err = nicks.Create(ctx, key, data)
	if err == nil {
		return true, nil
	} else if !errors.Is(err, jetstream.ErrKeyExists) {
		return false, unavailable(err)
	}

	entry, err := nicks.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		// released between our Create and Get; try once more
		if _, err = nicks.Create(ctx, key, data); err == nil {
			return true, nil
		} else if errors.Is(err, jetstream.ErrKeyExists) {
			return false, ErrNickInUse
		}
		return false, unavailable(err)
	} else if err != nil {
		return false, unavailable(err)
	}
	var current NickClaim
	if err := json.Unmarshal(entry.Value(), &current); err != nil {
		// unreadable claim: take it over
		current.Node = ""
	}
	if current.Node == claim.Node {
		return false, nil
	}
	if current.Node != "" {
		alive, err := n.nodeAlive(ctx, nodes, current.Node)
		if err != nil {
			return false, unavailable(err)
		}
		if alive {
			return false, ErrNickInUse
		}
	}
	// the owner is gone: take the claim over, unless someone beats us to it
	if _, err := nicks.Update(ctx, key, data, entry.Revision()); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return false, ErrNickInUse
		}
		return false, unavailable(err)
	}
	return true, nil
}

// Release gives up this node's claim on skeleton, if it holds one.
func (n *Nicks) Release(skeleton string) error {
	nicks, _ := n.buckets()
	if nicks == nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(n.ctx, kvTimeout)
	defer cancel()
	key := nickKey(skeleton)
	entry, err := nicks.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return nil
	} else if err != nil {
		return unavailable(err)
	}
	var current NickClaim
	if json.Unmarshal(entry.Value(), &current) == nil && current.Node != n.c.config.NodeName {
		return nil // not ours
	}
	err = nicks.Delete(ctx, key, jetstream.LastRevision(entry.Revision()))
	if err != nil && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return unavailable(err)
	}
	return nil
}

// Lookup returns the current claim on skeleton, if any.
func (n *Nicks) Lookup(skeleton string) (claim NickClaim, found bool, err error) {
	nicks, _ := n.buckets()
	if nicks == nil {
		return claim, false, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(n.ctx, kvTimeout)
	defer cancel()
	entry, err := nicks.Get(ctx, nickKey(skeleton))
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return claim, false, nil
	} else if err != nil {
		return claim, false, unavailable(err)
	}
	if err := json.Unmarshal(entry.Value(), &claim); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

// Owned returns the skeletons this node currently holds claims on.
func (n *Nicks) Owned() (skeletons []string, err error) {
	nicks, _ := n.buckets()
	if nicks == nil {
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(n.ctx, 4*kvTimeout)
	defer cancel()
	w, err := nicks.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, unavailable(err)
	}
	defer w.Stop()
	for {
		select {
		case entry := <-w.Updates():
			if entry == nil {
				return skeletons, nil // caught up with the current values
			}
			skeleton, ok := skeletonFromKey(entry.Key())
			var claim NickClaim
			if ok && json.Unmarshal(entry.Value(), &claim) == nil && claim.Node == n.c.config.NodeName {
				skeletons = append(skeletons, skeleton)
			}
		case <-ctx.Done():
			return nil, unavailable(ctx.Err())
		}
	}
}
