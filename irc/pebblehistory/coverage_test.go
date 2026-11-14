// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package pebblehistory

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
)

func testLogger(t *testing.T) *logger.Manager {
	t.Helper()
	lm, err := logger.NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	return lm
}

func TestOpenErrors(t *testing.T) {
	lm := testLogger(t)

	if _, err := NewPebbleDatabase(lm, Config{Enabled: true}); err == nil {
		t.Error("expected an error for an empty path")
	}

	// path is a regular file, not a directory
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, []byte("x"), 0600)
	if _, err := NewPebbleDatabase(lm, Config{Enabled: true, Path: file}); err == nil {
		t.Error("expected an error opening a file as a database")
	}

	// a database written by an incompatible future schema is refused
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	db.Set(keySchema, []byte("99"), pebble.Sync)
	db.Close()
	if _, err := NewPebbleDatabase(lm, Config{Enabled: true, Path: dir}); err == nil {
		t.Error("expected an incompatible schema error")
	}
}

func TestSyncAndCacheOptions(t *testing.T) {
	p := newTestDB(t, Config{Sync: true, CacheSizeMB: 8})
	if p.writeOpts() != pebble.Sync {
		t.Error("expected synchronous writes")
	}
	addChannel(t, p, "#a", 2, "")
	p.SetConfig(Config{Path: p.getConfig().Path})
	if p.writeOpts() != pebble.NoSync {
		t.Error("expected asynchronous writes after SetConfig")
	}
}

func TestInvalidParams(t *testing.T) {
	p := newTestDB(t, Config{})
	item := mkItem("x", "x", base)
	assertEqual(t, p.AddChannelItem("", item, ""), utils.ErrInvalidParams)
	assertEqual(t, p.AddDirectMessage("", "alice", "bob", "", item), utils.ErrInvalidParams)
	assertEqual(t, p.AddDirectMessage("alice", "alice", "", "", item), utils.ErrInvalidParams)
	// Forget with no account is a no-op
	p.Forget("")
	if res, err := p.ListChannels(nil); res != nil || err != nil {
		t.Error("expected empty result for no channels")
	}
}

func TestClosedDatabase(t *testing.T) {
	p := newTestDB(t, Config{TrackAccountMessages: true})
	addChannel(t, p, "#a", 2, "alice")
	seq := p.MakeSequence("#a", "", time.Time{})
	p.Close()
	assertEqual(t, p.Close(), nil) // closing twice is fine

	item := mkItem("y", "y", base)
	assertEqual(t, p.AddChannelItem("#a", item, ""), errClosed)
	assertEqual(t, p.AddDirectMessage("a", "a", "b", "b", item), errClosed)
	assertEqual(t, p.DeleteMsgid("#a-0"), history.ErrNotFound)
	_, _, err := p.LoadMsgid("#a-0")
	assertEqual(t, err, history.ErrNotFound)

	res, err := seq.Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, len(res), 0)
	assertEqual(t, err, nil)
	res, err = seq.Between(history.Selector{Msgid: "#a-0"}, history.Selector{}, 10)
	assertEqual(t, len(res), 0)
	assertEqual(t, err, nil)
	res, err = seq.Between(history.Selector{}, history.Selector{Msgid: "#a-1"}, 10)
	assertEqual(t, len(res), 0)
	assertEqual(t, err, nil)

	targets, err := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	assertEqual(t, len(targets), 0)
	assertEqual(t, err, nil)
	channels, err := p.ListChannels([]string{"#a"})
	assertEqual(t, len(channels), 0)
	assertEqual(t, err, nil)

	var buf bytes.Buffer
	p.Export("alice", &buf)
	assertEqual(t, buf.Len(), 0)
	p.Forget("alice") // logs, doesn't panic

	count, err := p.doCleanup(time.Now())
	assertEqual(t, count, 0)
	assertEqual(t, err, errClosed)
	found, err := p.doForget()
	assertEqual(t, found, false)
	assertEqual(t, err, errClosed)
	_, err = p.doForgetIteration("alice")
	assertEqual(t, err, errClosed)
}

func TestSequenceMisc(t *testing.T) {
	p := newTestDB(t, Config{})
	addChannel(t, p, "#a", 3, "")
	cutoff := base.Add(time.Hour)
	seq := p.MakeSequence("#a", "", cutoff)
	assertEqual(t, seq.Cutoff(), cutoff)
	assertEqual(t, seq.Ephemeral(), false)

	// an unknown end msgid yields nothing
	res, err := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{Msgid: "nope"}, 10)
	assertEqual(t, len(res), 0)
	assertEqual(t, err, nil)
	// a zero limit yields nothing
	res, _ = p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 0)
	assertEqual(t, len(res), 0)
	// an impossible range (after > before after normalization) yields nothing
	res, _ = p.betweenTimestamps(seqPrefix("#a"), base.Add(time.Minute), base.Add(time.Minute), time.Time{}, 10)
	assertEqual(t, len(res), 0)

	// a self-DM is stored once, in the sender's history
	p.AddDirectMessage("alice", "alice", "alice", "alice", mkItem("self", "note to self", base))
	res, _ = p.MakeSequence("alice", "alice", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(res), "note to self")

	// an older message doesn't move a correspondent's latest time backwards
	p.AddDirectMessage("alice", "alice", "bob", "", mkItem("new", "new", base.Add(time.Hour)))
	p.AddDirectMessage("alice", "alice", "bob", "", mkItem("old", "old", base))
	targets, _ := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	for _, target := range targets {
		if target.CfName == "bob" {
			assertEqual(t, target.Time, base.Add(time.Hour))
		}
	}
}

// writeRaw stores a key directly, bypassing the API, to simulate corruption
func writeRaw(t *testing.T, p *Pebble, key, value []byte) {
	t.Helper()
	if err := p.db.Set(key, value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptData(t *testing.T) {
	p := newTestDB(t, Config{TrackAccountMessages: true})
	rk := recordKey("bad", "#c")
	writeRaw(t, p, rk, []byte("{not json"))
	writeRaw(t, p, withSuffix(appendNano(seqPrefix("#c"), base.UnixNano()), rk), rk)
	writeRaw(t, p, withSuffix(appendNano(accountPrefix("mallory"), base.UnixNano()), rk), rk)

	if _, err := p.MakeSequence("#c", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10); err == nil {
		t.Error("expected an error reading a corrupt record")
	}
	if _, _, err := p.LoadMsgid("bad"); err == nil {
		t.Error("expected an error loading a corrupt record")
	}
	if _, err := p.MakeSequence("#c", "", time.Time{}).Between(history.Selector{Msgid: "bad"}, history.Selector{}, 10); err == nil {
		t.Error("expected an error resolving a corrupt start msgid")
	}
	if _, err := p.MakeSequence("#c", "", time.Time{}).Between(history.Selector{}, history.Selector{Msgid: "bad"}, 10); err == nil {
		t.Error("expected an error resolving a corrupt end msgid")
	}
	if err := p.DeleteMsgid("bad"); err == nil {
		t.Error("expected an error deleting a corrupt record")
	}
	var buf bytes.Buffer
	p.Export("mallory", &buf) // logs the error
	assertEqual(t, buf.Len(), 0)

	// a record whose JSON is valid but whose item isn't
	rk2 := recordKey("baditem", "#d")
	writeRaw(t, p, rk2, []byte(`{"n":1,"t":"#d","a":"eve","i":"\"not an item\""}`))
	writeRaw(t, p, withSuffix(appendNano(seqPrefix("#d"), 1), rk2), rk2)
	writeRaw(t, p, withSuffix(appendNano(accountPrefix("eve"), 1), rk2), rk2)
	if _, err := p.MakeSequence("#d", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10); err == nil {
		t.Error("expected an error unmarshaling a corrupt item")
	}
	p.Export("eve", &buf)
	assertEqual(t, buf.Len(), 0)

	// malformed index keys are skipped rather than crashing
	writeRaw(t, p, append(corrTimePrefix("zed"), 1, 2, 3), nil)
	writeRaw(t, p, append(appendNano(corrTimePrefix("zed"), 5), 0xff), nil)
	targets, err := p.ListCorrespondents("zed", time.Time{}, time.Time{}, 10)
	assertEqual(t, len(targets), 0)
	assertEqual(t, err, nil)
	writeRaw(t, p, append(seqPrefix("#short"), 1), nil)
	channels, _ := p.ListChannels([]string{"#short"})
	assertEqual(t, len(channels), 0)

	// a forget queue entry for a corrupt record surfaces the error
	_, err = p.doForgetIteration("mallory")
	if err == nil {
		t.Error("expected an error forgetting a corrupt record")
	}
	// a cleanup batch containing a corrupt record surfaces the error
	writeRaw(t, p, expireKey(1, rk), rk)
	if _, err := p.doCleanup(time.Now()); err == nil {
		t.Error("expected an error expiring a corrupt record")
	}
}

func TestCorruptCorrespondentEntry(t *testing.T) {
	p := newTestDB(t, Config{})
	// a truncated correspondents value is overwritten on the next message
	writeRaw(t, p, corrKey("alice", "bob"), []byte{1, 2})
	if err := p.AddDirectMessage("alice", "alice", "bob", "", mkItem("d", "d", base)); err != nil {
		t.Fatal(err)
	}
	targets, _ := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	assertEqual(t, len(targets), 1)
	// and is skipped during expiry
	writeRaw(t, p, corrKey("alice", "bob"), []byte{1, 2})
	p.doCleanup(time.Now())
}

func TestOrphanedIndexEntries(t *testing.T) {
	p := newTestDB(t, Config{TrackAccountMessages: true})
	// index entries whose record is gone are removed by cleanup and forget
	ghost := recordKey("ghost", "#a")
	ek := expireKey(1, ghost)
	ak := withSuffix(appendNano(accountPrefix("alice"), 1), ghost)
	sk := withSuffix(appendNano(seqPrefix("#a"), 1), ghost)
	writeRaw(t, p, ek, ghost)
	writeRaw(t, p, ak, ghost)
	writeRaw(t, p, sk, ghost)

	// reads skip entries whose record is missing
	res, err := p.MakeSequence("#a", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, len(res), 0)
	assertEqual(t, err, nil)
	var buf bytes.Buffer
	p.Export("alice", &buf)
	assertEqual(t, buf.Len(), 0)

	count, err := p.doCleanup(time.Now())
	assertEqual(t, count, 1)
	assertEqual(t, err, nil)
	if _, err := getValue(p.db, ek); err != pebble.ErrNotFound {
		t.Error("orphaned expiry entry was not removed")
	}
	count, err = p.doForgetIteration("alice")
	assertEqual(t, count, 1)
	assertEqual(t, err, nil)
	if _, err := getValue(p.db, ak); err != pebble.ErrNotFound {
		t.Error("orphaned account entry was not removed")
	}
	// nothing left to clean up
	count, _ = p.doCleanup(time.Now())
	assertEqual(t, count, 0)
}

func TestExportSkipsDMs(t *testing.T) {
	p := newTestDB(t, Config{TrackAccountMessages: true})
	p.AddDirectMessage("alice", "alice", "bob", "bob", mkItem("dm", "dm", base))
	p.AddChannelItem("#a", mkItem("ch", "ch", base), "alice")
	var buf bytes.Buffer
	p.Export("alice", &buf)
	assertEqual(t, bytes.Count(buf.Bytes(), []byte("\n")), 1)
}

func TestBackgroundLoops(t *testing.T) {
	old := cleanupPauseTime
	cleanupPauseTime = 20 * time.Millisecond
	defer func() { cleanupPauseTime = old }()

	p := newTestDB(t, Config{ExpireTime: time.Hour, TrackAccountMessages: true})
	// more than one cleanup batch, all old enough to expire
	for i := 0; i < cleanupBatchSize+50; i++ {
		p.AddChannelItem("#old", mkItem(fmt.Sprintf("o%d", i), "old", base.Add(time.Duration(i))), "alice")
	}
	p.AddChannelItem("#new", mkItem("fresh", "fresh", time.Now()), "alice")

	deadline := time.Now().Add(10 * time.Second)
	for {
		res, _ := p.MakeSequence("#old", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
		if len(res) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup loop did not expire old messages")
		}
		time.Sleep(10 * time.Millisecond)
	}
	res, _ := p.MakeSequence("#new", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
	assertEqual(t, texts(res), "fresh")

	// forget more than one batch's worth
	for i := 0; i < cleanupBatchSize+10; i++ {
		p.AddChannelItem("#f", mkItem(fmt.Sprintf("f%d", i), "f", time.Now().Add(time.Duration(i))), "bob")
	}
	p.Forget("bob")
	p.Forget("bob") // queued twice: both entries get processed
	for {
		res, _ := p.MakeSequence("#f", "", time.Time{}).Between(history.Selector{}, history.Selector{}, 10)
		if len(res) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("forget loop did not delete messages")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// closing stops the loops
	p.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestLoopsRecoverFromPanics(t *testing.T) {
	old := cleanupPauseTime
	cleanupPauseTime = 10 * time.Millisecond
	defer func() { cleanupPauseTime = old }()

	// each loop panics on its first iteration; it must recover and restart
	var mu sync.Mutex
	calls := map[string]int{}
	restarted := make(chan string, 16)
	hook := func(loop string) {
		mu.Lock()
		calls[loop]++
		n := calls[loop]
		mu.Unlock()
		if n == 1 {
			panic("injected " + loop + " panic")
		}
		restarted <- loop
	}
	loopHook.Store(&hook)
	defer loopHook.Store(nil)

	newTestDB(t, Config{})
	seen := map[string]bool{}
	timeout := time.After(5 * time.Second)
	for !seen["cleanup"] || !seen["forget"] {
		select {
		case loop := <-restarted:
			seen[loop] = true
		case <-timeout:
			t.Fatalf("loops did not recover from panics, saw %v", seen)
		}
	}
}

func TestEncodingHelpers(t *testing.T) {
	if _, _, ok := readStr([]byte{0}); ok {
		t.Error("readStr accepted a truncated length")
	}
	if _, _, ok := readStr([]byte{0, 5, 'a'}); ok {
		t.Error("readStr accepted a truncated string")
	}
	if _, _, ok := readNano([]byte{1, 2, 3}); ok {
		t.Error("readNano accepted a truncated timestamp")
	}
	nano, _, _ := readNano(appendNano(nil, -5))
	assertEqual(t, nano, 0)
	assertEqual(t, prefixEnd([]byte{0xff, 0xff}) == nil, true)
	assertEqual(t, prefixEnd([]byte{1, 0xff}), []byte{2})
}

func TestLoggers(t *testing.T) {
	p := newTestDB(t, Config{})
	l := pebbleLogger{p.logger}
	l.Infof("info %d", 1)
	l.Errorf("error %d", 2)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Fatalf should panic")
			}
		}()
		l.Fatalf("fatal %d", 3)
	}()
	assertEqual(t, p.logError("ctx", nil), false)
	assertEqual(t, p.logError("ctx", errors.New("boom")), true)
	assertEqual(t, p.logError("ctx", errClosed), true)
}

func TestExpireConversationTwiceInOneBatch(t *testing.T) {
	p := newTestDB(t, Config{})
	// two old DMs in one conversation expire together; the correspondents
	// entry is removed by the first and already gone for the second
	p.AddDirectMessage("alice", "alice", "bob", "", mkItem("d1", "d1", base))
	p.AddDirectMessage("alice", "alice", "bob", "", mkItem("d2", "d2", base))
	count, err := p.doCleanup(time.Now())
	assertEqual(t, count, 2)
	assertEqual(t, err, nil)
	targets, _ := p.ListCorrespondents("alice", time.Time{}, time.Time{}, 10)
	assertEqual(t, len(targets), 0)

	// a conversation whose correspondents entry is already gone still expires
	p.AddDirectMessage("alice", "alice", "carol", "", mkItem("d3", "d3", base))
	if err := p.db.Delete(corrKey("alice", "carol"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	count, err = p.doCleanup(time.Now())
	assertEqual(t, count, 1)
	assertEqual(t, err, nil)
}

func TestForgetWakeupCoalesces(t *testing.T) {
	p := newTestDB(t, Config{})
	// repeated requests while the forgetter is already awake don't block
	for i := 0; i < 100; i++ {
		p.Forget(fmt.Sprintf("user%d", i))
	}
}

func TestForgetLoopRetriesOnError(t *testing.T) {
	old := cleanupPauseTime
	cleanupPauseTime = 10 * time.Millisecond
	defer func() { cleanupPauseTime = old }()

	p := newTestDB(t, Config{TrackAccountMessages: true})
	rk := recordKey("bad", "#c")
	writeRaw(t, p, rk, []byte("{not json"))
	writeRaw(t, p, withSuffix(appendNano(accountPrefix("mallory"), 1), rk), rk)

	retries := make(chan struct{}, 64)
	// the forgetter keeps failing on the corrupt record, pausing between attempts
	p.Forget("mallory")
	go func() {
		for i := 0; i < 3; i++ {
			if _, err := p.doForget(); err != nil {
				retries <- struct{}{}
			}
		}
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-retries:
		case <-time.After(5 * time.Second):
			t.Fatal("expected forget to keep failing on a corrupt record")
		}
	}
	time.Sleep(50 * time.Millisecond)
	// closing stops the loop while it's waiting to retry
	p.Close()
}
