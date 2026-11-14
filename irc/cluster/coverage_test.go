// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/pebblehistory"
)

func quietLogger() *logger.Manager {
	lm, _ := logger.NewManager(nil)
	return lm
}

// startSingle starts a non-clustered embedded node
func startSingle(t *testing.T, name string) *Cluster {
	t.Helper()
	config := Config{
		Enabled:  true,
		NodeName: name,
		Embedded: EmbeddedConfig{StoreDir: t.TempDir()},
		History:  HistoryConfig{Enabled: true},
	}
	if err := config.Postprocess(); err != nil {
		t.Fatal(err)
	}
	c, err := Start(quietLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func newPebble(t *testing.T) *pebblehistory.Pebble {
	t.Helper()
	p, err := pebblehistory.NewPebbleDatabase(quietLogger(),
		pebblehistory.Config{Enabled: true, Path: t.TempDir(), TrackAccountMessages: true})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// external starts a standalone NATS server, as if run outside ergo
func external(t *testing.T, port int, jetStream bool) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: jetStream,
		StoreDir:  t.TempDir(),
		NoSigs:    true,
		NoLog:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("external NATS server did not start")
	}
	t.Cleanup(ns.Shutdown)
	return ns
}

func waitActive(t *testing.T, r *ReplicatedHistory) {
	t.Helper()
	eventually(t, "replication to become active", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.stream != nil
	})
}

func TestSingleNodeAccessors(t *testing.T) {
	c := startSingle(t, "solo")
	if c.JetStream() == nil || c.Conn() == nil || !c.Conn().IsConnected() {
		t.Fatal("expected a live connection")
	}
	// the in-process connection authenticates with generated credentials
	if c.clientUser == "" || c.clientPass == "" {
		t.Error("expected generated client credentials")
	}
	// closing a cluster that never started is a no-op
	(&Cluster{}).Close()
}

func TestEmbeddedClientListener(t *testing.T) {
	port := freePort(t)
	config := Config{
		Enabled:  true,
		NodeName: "listener",
		Embedded: EmbeddedConfig{
			StoreDir:       t.TempDir(),
			ClientListen:   fmt.Sprintf("127.0.0.1:%d", port),
			ClientUsername: "ops",
			ClientPassword: "hunter2",
		},
	}
	if err := config.Postprocess(); err != nil {
		t.Fatal(err)
	}
	c, err := Start(quietLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// an external client can connect with the configured credentials...
	lm := quietLogger()
	ext, err := Start(lm, Config{Enabled: true, NodeName: "tool", URL: fmt.Sprintf("nats://ops:hunter2@127.0.0.1:%d", port)})
	if err != nil {
		t.Fatal(err)
	}
	ext.Close()
	// ...but not without them
	if _, err := Start(lm, Config{Enabled: true, NodeName: "tool", URL: fmt.Sprintf("nats://127.0.0.1:%d", port)}); err == nil {
		t.Error("expected unauthenticated connection to be refused")
	}
}

func TestStartErrors(t *testing.T) {
	lm := quietLogger()
	cases := map[string]Config{
		"bad listen":        {NodeName: "a", Embedded: EmbeddedConfig{StoreDir: t.TempDir(), Listen: "nope"}},
		"bad listen port":   {NodeName: "a", Embedded: EmbeddedConfig{StoreDir: t.TempDir(), Listen: "127.0.0.1:x"}},
		"bad client listen": {NodeName: "a", Embedded: EmbeddedConfig{StoreDir: t.TempDir(), ClientListen: "nope"}},
		"bad route":         {NodeName: "a", Embedded: EmbeddedConfig{StoreDir: t.TempDir(), Listen: "127.0.0.1:0", Routes: []string{"://bad"}}},
		"invalid server":    {NodeName: "has space", Embedded: EmbeddedConfig{StoreDir: t.TempDir()}},
		"unreachable":       {NodeName: "a", URL: "nats://127.0.0.1:1"},
		"missing creds":     {NodeName: "a", URL: "nats://127.0.0.1:1", CredsFile: "/nonexistent/creds"},
	}
	for name, config := range cases {
		config.Enabled = true
		if c, err := Start(lm, config); err == nil {
			c.Close()
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestStartupTimeout(t *testing.T) {
	old := startupTimeout
	startupTimeout = 500 * time.Millisecond
	defer func() { startupTimeout = old }()

	// the route port is already taken, so the server never becomes ready
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	config := Config{Enabled: true, NodeName: "a", Embedded: EmbeddedConfig{
		StoreDir:    t.TempDir(),
		ClusterName: "ergo",
		Listen:      l.Addr().String(),
	}}
	c, err := Start(quietLogger(), config)
	if err == nil {
		c.Close()
		t.Fatal("expected a startup timeout")
	}
	t.Log(err)
}

func TestJetStreamFailsAfterListenersAreUp(t *testing.T) {
	// clustered JetStream without routes fails once the listeners are already
	// up; startup must notice and fail rather than run without JetStream
	config := Config{Enabled: true, NodeName: "a", Embedded: EmbeddedConfig{
		StoreDir:    t.TempDir(),
		ClusterName: "ergo",
		Listen:      fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}}
	if c, err := Start(quietLogger(), config); err == nil {
		c.Close()
		t.Fatal("expected JetStream startup failure")
	}
}

func TestExternalNATSAndReconnect(t *testing.T) {
	port := freePort(t)
	ns := external(t, port, true)
	config := Config{Enabled: true, NodeName: "ext", URL: ns.ClientURL(), History: HistoryConfig{Enabled: true}}
	if err := config.Postprocess(); err != nil {
		t.Fatal(err)
	}
	c, err := Start(quietLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r := c.ReplicateHistory(newPebble(t))
	defer r.Close()
	waitActive(t, r)

	// restart the external server on the same port: the client reconnects
	ns.Shutdown()
	ns.WaitForShutdown()
	eventually(t, "disconnect", func() bool { return !c.Conn().IsConnected() })
	external(t, port, true)
	eventually(t, "reconnect", func() bool { return c.Conn().IsConnected() })
}

func TestReplicationSetupRetries(t *testing.T) {
	// a server without JetStream: stream setup fails and is retried until close
	ns := external(t, freePort(t), false)
	c, err := Start(quietLogger(), Config{Enabled: true, NodeName: "nojs", URL: ns.ClientURL(), Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := c.ReplicateHistory(newPebble(t))
	// writes still succeed locally while replication is down
	if err := r.AddChannelItem("#a", msg("m1", "local only", time.Now()), "alice"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second) // at least one failed setup attempt
	assertLatest(t, r, "#a", "[local only]")
	// closing while inactive doesn't wait for the queue to flush
	start := time.Now()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("close waited on an inactive queue")
	}
}

func assertLatest(t *testing.T, db history.Database, target, want string) {
	t.Helper()
	if got := fmt.Sprint(latest(db, target, "")); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestPublishRetriesWhenStreamMissing(t *testing.T) {
	c := startSingle(t, "solo")
	r := c.ReplicateHistory(newPebble(t))
	waitActive(t, r)

	// delete the stream out from under the publisher: publishes and purges fail
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.JetStream().DeleteStream(ctx, historyStream); err != nil {
		t.Fatal(err)
	}
	r.Forget("alice") // the purge fails first
	r.AddChannelItem("#a", msg("m1", "1", time.Now()), "alice")
	r.AddChannelItem("#a", msg("m2", "2", time.Now()), "alice")
	time.Sleep(1500 * time.Millisecond) // a retry or two
	// close gives up flushing after its deadline and reports the backlog
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

type failingDB struct {
	history.Database
	err error
}

func (f failingDB) AddChannelItem(string, history.Item, string) error { return f.err }
func (f failingDB) AddDirectMessage(string, string, string, string, history.Item) error {
	return f.err
}
func (f failingDB) DeleteMsgid(string) error { return f.err }

func TestLocalErrorsAreNotReplicated(t *testing.T) {
	c := startSingle(t, "solo")
	boom := errors.New("boom")
	r := c.ReplicateHistory(failingDB{history.NewNoopDatabase(), boom})
	defer r.Close()
	item := msg("m", "m", time.Now())
	assertEqual(t, r.AddChannelItem("#a", item, ""), boom)
	assertEqual(t, r.AddDirectMessage("a", "a", "b", "b", item), boom)
	assertEqual(t, r.DeleteMsgid("m"), boom)
	assertEqual(t, len(r.queue), 0)

	// applying remote events surfaces local errors too
	data, _ := history.MarshalItem(&item)
	assertEqual(t, r.applyEvent(&historyEvent{Op: opChannel, Target: "#a", Item: data}), boom)
	assertEqual(t, r.applyEvent(&historyEvent{Op: opDelete, Msgid: "m"}), boom)
}

func TestEnqueueEdgeCases(t *testing.T) {
	c := startSingle(t, "solo")
	local := newPebble(t)
	r := c.ReplicateHistory(local)
	defer r.Close()

	// unregistered DMs aren't stored, so nothing is replicated
	assertEqual(t, r.AddDirectMessage("a", "", "b", "", msg("m", "m", time.Now())), nil)
	// deleting an unknown msgid still replicates (it may exist elsewhere)
	assertEqual(t, r.DeleteMsgid("unknown"), history.ErrNotFound)
	r.Forget("") // no-op

	// an event that can't be marshaled is dropped
	r.enqueue(historyEvent{Op: opChannel, Item: []byte("{invalid")}, "ergo.history.add._", "", "")

	// a full queue drops events rather than blocking
	full := &ReplicatedHistory{Database: local, c: c, queue: make(chan outgoing)}
	full.enqueue(historyEvent{Op: opDelete, Msgid: "x"}, historyDelSubject, "", "")
}

func TestApplyEvents(t *testing.T) {
	c := startSingle(t, "solo")
	r := c.ReplicateHistory(newPebble(t))
	defer r.Close()

	item := msg("dm1", "hi", time.Now())
	data, _ := history.MarshalItem(&item)
	assertEqual(t, r.applyEvent(&historyEvent{Op: opDM, Sender: "a", SenderAccount: "a", Recipient: "b", RecipientAccount: "b", Item: data}), nil)
	if got := fmt.Sprint(latest(r, "b", "a")); got != "[hi]" {
		t.Fatalf("DM not applied: %s", got)
	}
	assertEqual(t, r.applyEvent(&historyEvent{Op: opDelete, Msgid: "dm1"}), nil)
	assertEqual(t, r.applyEvent(&historyEvent{Op: opDelete, Msgid: "dm1"}), nil) // already gone
	assertEqual(t, r.applyEvent(&historyEvent{Op: opForget, Account: "a"}), nil)
	if r.applyEvent(&historyEvent{Op: "bogus"}) == nil {
		t.Error("expected an error for an unknown op")
	}
	if r.applyEvent(&historyEvent{Op: opChannel, Target: "#a", Item: []byte(`"not an item"`)}) == nil {
		t.Error("expected an error for a malformed item")
	}
}

func TestApplyFromStream(t *testing.T) {
	c := startSingle(t, "solo")
	r := c.ReplicateHistory(newPebble(t))
	defer r.Close()
	waitActive(t, r)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js := c.JetStream()
	// malformed and failing events are terminated / redelivered without
	// blocking the events behind them
	js.Publish(ctx, historyDelSubject, []byte("not json"))
	js.Publish(ctx, historyDelSubject, []byte(`{"op":"bogus","origin":"other"}`))
	item := msg("remote1", "from elsewhere", time.Now())
	data, _ := history.MarshalItem(&item)
	ev := fmt.Sprintf(`{"op":"channel","origin":"other","target":"#a","item":%s}`, data)
	if _, err := js.Publish(ctx, addSubject(""), []byte(ev)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "remote event applied", func() bool {
		return fmt.Sprint(latest(r, "#a", "")) == "[from elsewhere]"
	})
}

func TestForgetReplicates(t *testing.T) {
	pa, pb := freePort(t), freePort(t)
	lm := quietLogger()
	start := func(name string, port, peer int) (*Cluster, *ReplicatedHistory) {
		config := Config{
			Enabled: true, NodeName: name,
			Embedded: EmbeddedConfig{
				StoreDir: t.TempDir(),
				Listen:   fmt.Sprintf("127.0.0.1:%d", port),
				Routes:   []string{fmt.Sprintf("nats-route://127.0.0.1:%d", peer)},
			},
			History: HistoryConfig{Enabled: true},
		}
		config.Postprocess()
		c, err := Start(lm, config)
		if err != nil {
			t.Fatal(err)
		}
		r := c.ReplicateHistory(newPebble(t))
		t.Cleanup(func() { r.Close(); c.Close() })
		return c, r
	}
	_, a := start("a", pa, pb)
	cb, b := start("b", pb, pa)

	a.AddChannelItem("#a", msg("m1", "alice says", time.Now()), "alice")
	a.AddChannelItem("#a", msg("m2", "bob says", time.Now().Add(time.Millisecond)), "bob")
	eventually(t, "replication to b", func() bool { return len(latest(b, "#a", "")) == 2 })

	// forgetting alice on b deletes her messages on a too, and purges them
	// from the stream so a new node can't backfill them
	b.Forget("alice")
	eventually(t, "forget on a", func() bool { return fmt.Sprint(latest(a, "#a", "")) == "[bob says]" })
	eventually(t, "forget on b", func() bool { return fmt.Sprint(latest(b, "#a", "")) == "[bob says]" })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cb.JetStream().Stream(ctx, historyStream)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "stream purge", func() bool {
		_, err := stream.GetLastMsgForSubject(ctx, addSubject("alice"))
		return errors.Is(err, jetstream.ErrMsgNotFound)
	})
}

func TestNatsLogger(t *testing.T) {
	l := &natsLogger{logger: quietLogger()}
	l.Noticef("n %d", 1)
	l.Warnf("w %d", 1)
	l.Errorf("e %d", 1)
	l.Fatalf("f %d", 1)
	l.Debugf("d %d", 1)
	l.Tracef("t %d", 1)
}

func TestConfigEqual(t *testing.T) {
	base := Config{
		Enabled: true, NodeName: "a", Replicas: 1,
		Embedded: EmbeddedConfig{Listen: "0.0.0.0:6222", Routes: []string{"nats-route://b:6222"}},
	}
	same := base
	same.Embedded.Routes = []string{"nats-route://b:6222"}
	if !base.Equal(&same) {
		t.Error("identical configs should be equal")
	}
	changes := []func(*Config){
		func(c *Config) { c.Enabled = false },
		func(c *Config) { c.NodeName = "b" },
		func(c *Config) { c.URL = "nats://x" },
		func(c *Config) { c.CredsFile = "x" },
		func(c *Config) { c.Replicas = 3 },
		func(c *Config) { c.History.Enabled = true },
		func(c *Config) { c.HistoryExpireTime = time.Hour },
		func(c *Config) { c.Embedded.StoreDir = "x" },
		func(c *Config) { c.Embedded.ClusterName = "x" },
		func(c *Config) { c.Embedded.Listen = "x" },
		func(c *Config) { c.Embedded.Routes = nil },
		func(c *Config) { c.Embedded.RouteUsername = "x" },
		func(c *Config) { c.Embedded.RoutePassword = "x" },
		func(c *Config) { c.Embedded.ClientListen = "x" },
		func(c *Config) { c.Embedded.ClientUsername = "x" },
		func(c *Config) { c.Embedded.ClientPassword = "x" },
	}
	for i, change := range changes {
		other := base
		other.Embedded.Routes = []string{"nats-route://b:6222"}
		change(&other)
		if base.Equal(&other) {
			t.Errorf("change %d should make configs unequal", i)
		}
	}
}

func TestConfigValidationBranches(t *testing.T) {
	disabled := Config{}
	assertEqual(t, disabled.Postprocess(), nil)

	bad := map[string]Config{
		"invalid listen":        {Embedded: EmbeddedConfig{Listen: "nope"}},
		"invalid client listen": {Embedded: EmbeddedConfig{ClientListen: "nope"}},
		"client creds mismatch": {Embedded: EmbeddedConfig{ClientUsername: "u"}},
		"listen without routes": {Embedded: EmbeddedConfig{Listen: "0.0.0.0:6222"}},
		"invalid route":         {Embedded: EmbeddedConfig{Listen: "0.0.0.0:6222", Routes: []string{"://bad"}}},
	}
	for name, c := range bad {
		c.Enabled, c.NodeName = true, "a"
		if c.Postprocess() == nil {
			t.Errorf("%s: should have been rejected", name)
		}
	}

	// an external URL skips the embedded server checks entirely
	ext := Config{Enabled: true, NodeName: "a", URL: "nats://nats:4222", Replicas: 3,
		Embedded: EmbeddedConfig{Listen: "nope"}}
	assertEqual(t, ext.Postprocess(), nil)
	assertEqual(t, ext.Embedded.StoreDir, "")

	clustered := Config{Enabled: true, NodeName: "a", Replicas: 3, History: HistoryConfig{StreamMaxAge: time.Minute},
		Embedded: EmbeddedConfig{Listen: "0.0.0.0:6222", Routes: []string{"nats-route://a:6222", "nats-route://b:6222"},
			ClientListen: "127.0.0.1:4222", ClientUsername: "u", ClientPassword: "p"}}
	assertEqual(t, clustered.Postprocess(), nil)
	assertEqual(t, clustered.Embedded.ClusterName, "ergo")
	assertEqual(t, clustered.History.StreamMaxAge, time.Minute) // explicit value kept
}

func assertEqual(t *testing.T, got, want interface{}) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
