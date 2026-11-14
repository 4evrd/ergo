// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// startNickNodes starts n clustered nodes with nick registries
func startNickNodes(t *testing.T, names ...string) []*Nicks {
	t.Helper()
	ports := make([]int, len(names))
	for i := range ports {
		ports[i] = freePort(t)
	}
	var result []*Nicks
	for i, name := range names {
		config := Config{
			Enabled:  true,
			NodeName: name,
			Embedded: EmbeddedConfig{
				StoreDir: t.TempDir(),
				Listen:   fmt.Sprintf("127.0.0.1:%d", ports[i]),
			},
			Nicks: NicksConfig{Enabled: true, NodeTimeout: 3 * time.Second},
		}
		if len(names) == 1 {
			// a single node runs unclustered
			config.Embedded.Listen = ""
		} else {
			// every node lists every node, itself included
			for _, p := range ports {
				config.Embedded.Routes = append(config.Embedded.Routes, fmt.Sprintf("nats-route://127.0.0.1:%d", p))
			}
		}
		if err := config.Postprocess(); err != nil {
			t.Fatal(err)
		}
		c, err := Start(quietLogger(), config)
		if err != nil {
			t.Fatal(err)
		}
		n := c.StartNicks()
		t.Cleanup(func() {
			n.Close()
			c.Close()
		})
		result = append(result, n)
	}
	for _, n := range result {
		if !n.WaitReady(30 * time.Second) {
			t.Fatal("nick registry did not become ready")
		}
	}
	return result
}

func TestNickClaims(t *testing.T) {
	nodes := startNickNodes(t, "a", "b")
	a, b := nodes[0], nodes[1]

	acquired, err := a.Claim("alice", NickClaim{Nick: "Alice", Account: "alice"})
	assertEqual(t, acquired, true)
	assertEqual(t, err, nil)

	// claiming again on the same node defers to local state
	acquired, err = a.Claim("alice", NickClaim{Nick: "ALICE"})
	assertEqual(t, acquired, false)
	assertEqual(t, err, nil)

	// another live node can't have it
	_, err = b.Claim("alice", NickClaim{Nick: "alice"})
	assertEqual(t, err, ErrNickInUse)

	claim, found, err := b.Lookup("alice")
	assertEqual(t, found, true)
	assertEqual(t, err, nil)
	assertEqual(t, claim.Node, "a")
	assertEqual(t, claim.Account, "alice")
	_, found, _ = b.Lookup("nobody")
	assertEqual(t, found, false)

	// only the owner can release it
	assertEqual(t, b.Release("alice"), nil)
	_, err = b.Claim("alice", NickClaim{Nick: "alice"})
	assertEqual(t, err, ErrNickInUse)
	assertEqual(t, a.Release("alice"), nil)
	assertEqual(t, a.Release("alice"), nil) // already released
	acquired, err = b.Claim("alice", NickClaim{Nick: "alice"})
	assertEqual(t, acquired, true)
	assertEqual(t, err, nil)

	// non-ASCII skeletons are encoded into valid keys
	acquired, err = a.Claim("ротато", NickClaim{Nick: "РОТАТО"})
	assertEqual(t, acquired, true)
	assertEqual(t, err, nil)

	owned, err := a.Owned()
	assertEqual(t, err, nil)
	assertEqual(t, owned, []string{"ротато"})
	owned, _ = b.Owned()
	assertEqual(t, owned, []string{"alice"})
}

func TestNickTakeoverFromGoneNode(t *testing.T) {
	nodes := startNickNodes(t, "a", "b", "c")
	a, b, c := nodes[0], nodes[1], nodes[2]

	if _, err := b.Claim("bob", NickClaim{Nick: "bob"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Claim("carol", NickClaim{Nick: "carol"}); err != nil {
		t.Fatal(err)
	}

	// b shuts down gracefully: it deregisters, so its nicks are free at once
	b.Close()
	acquired, err := a.Claim("bob", NickClaim{Nick: "bob"})
	assertEqual(t, acquired, true)
	assertEqual(t, err, nil)

	// c crashes (its heartbeat stops but it never deregisters): its nicks
	// stay taken until the heartbeat expires
	c.cancel()
	c.wg.Wait()
	_, err = a.Claim("carol", NickClaim{Nick: "carol"})
	assertEqual(t, err, ErrNickInUse)
	eventually(t, "c's heartbeat to expire", func() bool {
		acquired, err := a.Claim("carol", NickClaim{Nick: "carol"})
		return acquired && err == nil
	})
}

func TestNickCorruptClaimIsTakenOver(t *testing.T) {
	nodes := startNickNodes(t, "a")
	a := nodes[0]
	nicks, _ := a.buckets()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nicks.Put(ctx, nickKey("mangled"), []byte("not json")); err != nil {
		t.Fatal(err)
	}
	_, _, err := a.Lookup("mangled")
	if err == nil {
		t.Error("expected an error looking up a corrupt claim")
	}
	acquired, err := a.Claim("mangled", NickClaim{Nick: "mangled"})
	assertEqual(t, acquired, true)
	assertEqual(t, err, nil)
	// a corrupt claim is treated as ours to release
	if _, err := nicks.Put(ctx, nickKey("mangled"), []byte("not json")); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, a.Release("mangled"), nil)
	_, found, _ := a.Lookup("mangled")
	assertEqual(t, found, false)

	// keys that don't decode are ignored by Owned
	if _, err := nicks.Put(ctx, "n.zz", []byte(`{"node":"a"}`)); err != nil {
		t.Fatal(err)
	}
	owned, err := a.Owned()
	assertEqual(t, err, nil)
	assertEqual(t, slices.Contains(owned, "zz"), false)
}

func TestNicksNotReady(t *testing.T) {
	n := &Nicks{}
	assertEqual(t, n.Ready(), false)
	assertEqual(t, n.WaitReady(50*time.Millisecond), false)
	_, err := n.Claim("x", NickClaim{})
	assertEqual(t, err, ErrUnavailable)
	assertEqual(t, n.Release("x"), ErrUnavailable)
	_, _, err = n.Lookup("x")
	assertEqual(t, err, ErrUnavailable)
	_, err = n.Owned()
	assertEqual(t, err, ErrUnavailable)
}

func TestNicksSetupRetriesAndCloses(t *testing.T) {
	// without JetStream, the registry never becomes ready; closing stops it
	ns := external(t, freePort(t), false)
	c, err := Start(quietLogger(), Config{Enabled: true, NodeName: "nojs", URL: ns.ClientURL(), Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n := c.StartNicks()
	assertEqual(t, n.WaitReady(4*time.Second), false)
	n.Close()
}

func TestNicksUnavailableWhenDisconnected(t *testing.T) {
	ns := external(t, freePort(t), true)
	config := Config{Enabled: true, NodeName: "ext", URL: ns.ClientURL()}
	config.Postprocess()
	c, err := Start(quietLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n := c.StartNicks()
	defer n.Close()
	if !n.WaitReady(10 * time.Second) {
		t.Fatal("registry not ready")
	}
	if _, err := n.Claim("held", NickClaim{Nick: "held"}); err != nil {
		t.Fatal(err)
	}

	ns.Shutdown()
	ns.WaitForShutdown()
	_, err = n.Claim("someone", NickClaim{Nick: "someone"})
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("expected ErrUnavailable, got %v", err)
	}
	if err := n.Release("held"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("expected ErrUnavailable releasing, got %v", err)
	}
	if _, _, err := n.Lookup("held"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("expected ErrUnavailable looking up, got %v", err)
	}
	assertEqual(t, n.Ready(), false) // fails fast while disconnected
	n.heartbeat()                    // skipped while disconnected
}

func TestNicksCanceledContext(t *testing.T) {
	// with the registry's context canceled, every operation fails as unavailable
	nodes := startNickNodes(t, "a")
	n := nodes[0]
	if _, err := n.Claim("held", NickClaim{Nick: "held"}); err != nil {
		t.Fatal(err)
	}
	n.cancel()
	n.wg.Wait()
	_, err := n.Claim("new", NickClaim{Nick: "new"})
	assertEqual(t, errors.Is(err, ErrUnavailable), true)
	assertEqual(t, errors.Is(n.Release("held"), ErrUnavailable), true)
	_, _, err = n.Lookup("held")
	assertEqual(t, errors.Is(err, ErrUnavailable), true)
	_, err = n.Owned()
	assertEqual(t, errors.Is(err, ErrUnavailable), true)
	n.heartbeat() // logs a warning
}

func TestNickKeyEncoding(t *testing.T) {
	sk, ok := skeletonFromKey(nickKey("hello"))
	assertEqual(t, sk, "hello")
	assertEqual(t, ok, true)
	_, ok = skeletonFromKey("x.68656c6c6f")
	assertEqual(t, ok, false)
	_, ok = skeletonFromKey("n.zz")
	assertEqual(t, ok, false)
	_ = jetstream.ErrKeyNotFound
}

func TestNicksConfig(t *testing.T) {
	c := Config{Enabled: true, NodeName: "a"}
	assertEqual(t, c.Postprocess(), nil)
	assertEqual(t, c.Nicks.NodeTimeout, defaultNodeTimeout)
	assertEqual(t, c.Nicks.ReconcileInterval, 30*time.Second)
	assertEqual(t, c.Nicks.StartupWait, 30*time.Second)

	bad := Config{Enabled: true, NodeName: "a", Nicks: NicksConfig{NodeTimeout: time.Second}}
	if bad.Postprocess() == nil {
		t.Error("expected node-timeout below 3s to be rejected")
	}
	bad = Config{Enabled: true, NodeName: "a", Nicks: NicksConfig{ReconcileInterval: time.Millisecond}}
	if bad.Postprocess() == nil {
		t.Error("expected reconcile-interval below 1s to be rejected")
	}
	other := c
	other.Nicks.Enabled = true
	assertEqual(t, c.Equal(&other), false)
}
