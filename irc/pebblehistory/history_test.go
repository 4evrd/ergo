// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package pebblehistory

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
)

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestDB(t *testing.T, config Config) *Pebble {
	t.Helper()
	lm, err := logger.NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	config.Enabled = true
	if config.Path == "" {
		config.Path = t.TempDir()
	}
	p, err := NewPebbleDatabase(lm, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func mkItem(msgid, text string, at time.Time) history.Item {
	return history.Item{
		Type:        history.Privmsg,
		Nick:        "alice!a@host",
		AccountName: "alice",
		Message: utils.SplitMessage{
			Message: text,
			Msgid:   msgid,
			Time:    at,
		},
	}
}

func texts(items []history.Item) string {
	var out []string
	for _, item := range items {
		out = append(out, item.Message.Message)
	}
	return strings.Join(out, ",")
}

func addChannel(t *testing.T, p *Pebble, channel string, n int, account string) {
	t.Helper()
	for i := 0; i < n; i++ {
		item := mkItem(fmt.Sprintf("%s-%d", channel, i), fmt.Sprintf("m%d", i), base.Add(time.Duration(i)*time.Second))
		if err := p.AddChannelItem(channel, item, account); err != nil {
			t.Fatal(err)
		}
	}
}

func assertEqual(t *testing.T, got, want interface{}) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestChannelQueries(t *testing.T) {
	p := newTestDB(t, Config{})
	addChannel(t, p, "#a", 10, "alice")
	addChannel(t, p, "#b", 3, "alice")
	seq := p.MakeSequence("#a", "", time.Time{})

	// LATEST
	items, err := seq.Between(history.Selector{}, history.Selector{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, texts(items), "m7,m8,m9")

	// BEFORE msgid
	items, _ = seq.Between(history.Selector{}, history.Selector{Msgid: "#a-5"}, 2)
	assertEqual(t, texts(items), "m3,m4")

	// AFTER msgid
	items, _ = seq.Between(history.Selector{Msgid: "#a-5"}, history.Selector{}, 2)
	assertEqual(t, texts(items), "m6,m7")

	// BETWEEN going forwards and backwards (exclusive bounds)
	items, _ = seq.Between(history.Selector{Msgid: "#a-2"}, history.Selector{Msgid: "#a-6"}, 10)
	assertEqual(t, texts(items), "m3,m4,m5")
	items, _ = seq.Between(history.Selector{Msgid: "#a-6"}, history.Selector{Msgid: "#a-2"}, 2)
	assertEqual(t, texts(items), "m4,m5")

	// timestamps
	items, _ = seq.Between(history.Selector{Time: base.Add(7 * time.Second)}, history.Selector{}, 10)
	assertEqual(t, texts(items), "m8,m9")

	// AROUND
	items, _ = seq.Around(history.Selector{Msgid: "#a-5"}, 4)
	if len(items) == 0 {
		t.Fatal("expected AROUND results")
	}

	// unknown msgid yields no results, not an error
	items, err = seq.Between(history.Selector{Msgid: "nope"}, history.Selector{}, 10)
	assertEqual(t, len(items), 0)
	assertEqual(t, err, nil)

	// cutoff
	cut := p.MakeSequence("#a", "", base.Add(7*time.Second))
	items, _ = cut.Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "m8,m9")

	// channels are isolated from each other
	items, _ = p.MakeSequence("#b", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "m0,m1,m2")

	listing, err := p.ListChannels([]string{"#a", "#b", "#empty"})
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(listing), 2)
	assertEqual(t, listing[0].Time, base.Add(9*time.Second))
	assertEqual(t, listing[1].Time, base.Add(2*time.Second))
}

func TestIdempotentAndSharedMsgid(t *testing.T) {
	p := newTestDB(t, Config{})
	// the same item stored twice in one channel is only kept once
	item := mkItem("dup", "hello", base)
	for i := 0; i < 3; i++ {
		if err := p.AddChannelItem("#a", item, ""); err != nil {
			t.Fatal(err)
		}
	}
	items, _ := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, len(items), 1)

	// but a NICK/QUIT shares its msgid across channels and must be kept in each
	quit := mkItem("quit1", "bye", base.Add(time.Second))
	p.AddChannelItem("#a", quit, "")
	p.AddChannelItem("#b", quit, "")
	items, _ = p.MakeSequence("#b", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "bye")
	items, _ = p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "hello,bye")

	// redacting a shared msgid removes every copy
	if err := p.DeleteMsgid("quit1"); err != nil {
		t.Fatal(err)
	}
	items, _ = p.MakeSequence("#b", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, len(items), 0)
	assertEqual(t, p.DeleteMsgid("quit1"), history.ErrNotFound)
}

func TestLoadMsgid(t *testing.T) {
	p := newTestDB(t, Config{})
	addChannel(t, p, "#a", 3, "")
	channel, item, err := p.LoadMsgid("#a-1")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, channel, "#a")
	assertEqual(t, item.Message.Message, "m1")
	assertEqual(t, item.Message.Time, base.Add(time.Second))
	_, _, err = p.LoadMsgid("missing")
	assertEqual(t, err, history.ErrNotFound)
}

func TestDirectMessages(t *testing.T) {
	p := newTestDB(t, Config{})
	dm := func(msgid, from, fromAcct, to, toAcct string, sec int) {
		t.Helper()
		item := mkItem(msgid, msgid, base.Add(time.Duration(sec)*time.Second))
		if err := p.AddDirectMessage(from, fromAcct, to, toAcct, item); err != nil {
			t.Fatal(err)
		}
	}
	dm("d1", "alice", "alice", "bob", "bob", 1)
	dm("d2", "bob", "bob", "alice", "alice", 2)
	dm("d3", "alice", "alice", "carol", "", 3)
	dm("d4", "dave", "", "eve", "", 4) // neither side registered: not stored
	dm("d5", "alice", "alice", "bob", "bob", 5)

	items, _ := p.MakeSequence("alice", "bob", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "d1,d2,d5")
	items, _ = p.MakeSequence("bob", "alice", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "d1,d2,d5")
	items, _ = p.MakeSequence("alice", "carol", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "d3")
	_, _, err := p.LoadMsgid("d4")
	assertEqual(t, err, history.ErrNotFound)

	// correspondents are ordered by latest message, and updated in place
	targets, err := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(targets), 2)
	assertEqual(t, targets[0].CfName, "carol")
	assertEqual(t, targets[1].CfName, "bob")
	assertEqual(t, targets[1].Time, base.Add(5*time.Second))

	// paging by time
	targets, _ = p.ListCorrespondents("alice", time.Time{}, base.Add(4*time.Second), 10)
	assertEqual(t, len(targets), 1)
	assertEqual(t, targets[0].CfName, "carol")
}

func TestExpiry(t *testing.T) {
	p := newTestDB(t, Config{})
	addChannel(t, p, "#a", 10, "")
	p.AddDirectMessage("alice", "alice", "bob", "bob", mkItem("d1", "old", base))

	count, err := p.doCleanup(base.Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, count, 6) // m0..m4 + d1

	items, _ := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 100)
	assertEqual(t, texts(items), "m5,m6,m7,m8,m9")
	targets, _ := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	assertEqual(t, len(targets), 0)
	_, _, err = p.LoadMsgid("#a-0")
	assertEqual(t, err, history.ErrNotFound)
}

func TestForgetAndExport(t *testing.T) {
	p := newTestDB(t, Config{TrackAccountMessages: true})
	addChannel(t, p, "#a", 3, "alice")
	p.AddChannelItem("#a", mkItem("bobmsg", "from bob", base.Add(time.Minute)), "bob")

	var buf bytes.Buffer
	p.Export("alice", &buf)
	assertEqual(t, strings.Count(buf.String(), "\n"), 3)

	p.Forget("alice")
	deadline := time.Now().Add(5 * time.Second)
	for {
		items, _ := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 100)
		if texts(items) == "from bob" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forget did not complete, still have %s", texts(items))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	p := newTestDB(t, Config{Path: dir})
	addChannel(t, p, "#a", 3, "")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// operations after close fail gracefully rather than panicking
	_, _, err := p.LoadMsgid("#a-0")
	assertEqual(t, err, history.ErrNotFound)

	p = newTestDB(t, Config{Path: dir})
	items, _ := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(items), "m0,m1,m2")
}
