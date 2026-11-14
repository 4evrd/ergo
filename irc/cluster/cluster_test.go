// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/pebblehistory"
	"github.com/ergochat/ergo/irc/utils"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type testNode struct {
	cluster *Cluster
	db      *ReplicatedHistory
}

func startNode(t *testing.T, name string, port int, peers []int) *testNode {
	t.Helper()
	lm := quietLogger()
	config := Config{
		Enabled:  true,
		NodeName: name,
		Embedded: EmbeddedConfig{
			StoreDir:      t.TempDir(),
			Listen:        fmt.Sprintf("127.0.0.1:%d", port),
			RouteUsername: "ergo",
			RoutePassword: "secret",
		},
		History: HistoryConfig{Enabled: true},
	}
	for _, p := range peers {
		config.Embedded.Routes = append(config.Embedded.Routes, fmt.Sprintf("nats-route://127.0.0.1:%d", p))
	}
	if err := config.Postprocess(); err != nil {
		t.Fatal(err)
	}
	c, err := Start(lm, config)
	if err != nil {
		t.Fatal(err)
	}
	local, err := pebblehistory.NewPebbleDatabase(lm, pebblehistory.Config{Enabled: true, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{cluster: c, db: c.ReplicateHistory(local)}
	t.Cleanup(func() {
		n.db.Close()
		n.cluster.Close()
	})
	return n
}

func latest(db history.Database, target, correspondent string) []string {
	items, _ := db.MakeSequence(target, correspondent, time.Time{}).Between(history.Selector{}, history.Selector{}, 100)
	var out []string
	for _, item := range items {
		out = append(out, item.Message.Message)
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func msg(msgid, text string, at time.Time) history.Item {
	return history.Item{
		Type: history.Privmsg,
		Nick: "alice!a@host",
		Message: utils.SplitMessage{
			Message: text,
			Msgid:   msgid,
			Time:    at,
		},
	}
}

func TestHistoryReplication(t *testing.T) {
	pa, pb := freePort(t), freePort(t)
	a := startNode(t, "a", pa, []int{pb})
	b := startNode(t, "b", pb, []int{pa})

	now := time.Now().UTC()
	if err := a.db.AddChannelItem("#chan", msg("m1", "hello from a", now), "alice"); err != nil {
		t.Fatal(err)
	}
	if err := b.db.AddChannelItem("#chan", msg("m2", "hello from b", now.Add(time.Millisecond)), "bob"); err != nil {
		t.Fatal(err)
	}
	if err := a.db.AddDirectMessage("alice", "alice", "bob", "bob", msg("d1", "psst", now)); err != nil {
		t.Fatal(err)
	}

	for _, n := range []*testNode{a, b} {
		eventually(t, "channel history on node "+n.cluster.NodeName(), func() bool {
			return fmt.Sprint(latest(n.db, "#chan", "")) == "[hello from a hello from b]"
		})
	}
	eventually(t, "DM on node b", func() bool {
		return fmt.Sprint(latest(b.db, "bob", "alice")) == "[psst]"
	})

	// redaction on one node propagates
	if err := b.db.DeleteMsgid("m1"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delete on node a", func() bool {
		return fmt.Sprint(latest(a.db, "#chan", "")) == "[hello from b]"
	})
}

func TestNodeCatchesUp(t *testing.T) {
	pa, pb := freePort(t), freePort(t)
	a := startNode(t, "a", pa, []int{pb})
	b := startNode(t, "b", pb, []int{pa})

	now := time.Now().UTC()
	for i := 0; i < 20; i++ {
		a.db.AddChannelItem("#chan", msg(fmt.Sprintf("m%d", i), fmt.Sprintf("%d", i), now.Add(time.Duration(i)*time.Millisecond)), "")
	}
	eventually(t, "node b to see history", func() bool { return len(latest(b.db, "#chan", "")) == 20 })

	// a third node joining later backfills everything from the stream
	pc := freePort(t)
	c := startNode(t, "c", pc, []int{pa, pb})
	eventually(t, "node c to backfill", func() bool { return len(latest(c.db, "#chan", "")) == 20 })
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{Enabled: true, NodeName: "has.dot"},
		{Enabled: true, NodeName: "a", Replicas: 2},
		{Enabled: true, NodeName: "a", Replicas: 3},                                              // single node can't have 3 replicas
		{Enabled: true, NodeName: "a", Embedded: EmbeddedConfig{Routes: []string{"nats://x:1"}}}, // routes without listen
		{Enabled: true, NodeName: "a", Embedded: EmbeddedConfig{Listen: "0.0.0.0:6222", RouteUsername: "u"}},
	}
	for i, c := range bad {
		if c.Postprocess() == nil {
			t.Errorf("config %d should have been rejected", i)
		}
	}
	good := Config{Enabled: true, NodeName: "node-1", HistoryExpireTime: time.Hour}
	if err := good.Postprocess(); err != nil {
		t.Fatal(err)
	}
	if good.Replicas != 1 || good.Embedded.StoreDir == "" || good.History.StreamMaxAge != time.Hour {
		t.Errorf("defaults not applied: %+v", good)
	}
}
