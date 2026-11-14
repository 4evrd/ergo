// Copyright (c) 2026 4evrd contributors
// released under the MIT license

// Package pebblehistory implements persistent history storage on top of
// Pebble, an embedded LSM key-value store. It needs no external database
// server and implements the same semantics as the SQL backends.
//
// Records are keyed by (msgid, target) and inserts are idempotent: storing an
// item that already exists is a no-op. This makes it safe to replay or
// replicate writes between nodes.
package pebblehistory

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
)

const (
	// Enabled is true when Pebble support is compiled in (it always is)
	Enabled = true

	latestSchema = "1"

	cleanupBatchSize = 500
	cleanupPauseTime = 10 * time.Minute

	defaultCacheSizeMB = 64
)

// Key prefixes. Variable-length components are encoded with a uint16 length
// prefix (see appendStr), and timestamps as big-endian uint64 nanoseconds, so
// that all keys for one target sort in time order.
//
//	m <msgid> <target>                    -> record (target is "" for DMs)
//	s <channel> <nano> <msgid> <target>   -> record key  (channel sequence)
//	c <acct> <corr> <nano> <msgid> <tgt>  -> record key  (DM conversation)
//	r <acct> <corr>                       -> nano        (latest DM per correspondent)
//	R <acct> <nano> <corr>                -> ""          (correspondents by time)
//	e <nano> <msgid> <target>             -> record key  (expiry order)
//	a <account> <nano> <msgid> <target>   -> record key  (messages by sender account)
//	f <nano> <account>                    -> ""          (forget queue)
const (
	pfxMeta     = 0x00
	pfxRecord   = 'm'
	pfxSeq      = 's'
	pfxConv     = 'c'
	pfxCorr     = 'r'
	pfxCorrTime = 'R'
	pfxExpire   = 'e'
	pfxAccount  = 'a'
	pfxForget   = 'f'
)

var (
	keySchema = []byte{pfxMeta, 's', 'c', 'h', 'e', 'm', 'a'}

	errClosed = errors.New("pebble history database is closed")
)

type e struct{}

// conv is one side of a stored direct message: the account whose history
// contains it, and the correspondent (casefolded nick) on the other side.
type conv struct {
	Target        string `json:"t"`
	Correspondent string `json:"c"`
}

type record struct {
	Nano    int64           `json:"n"`
	Target  string          `json:"t,omitempty"` // casefolded channel, for channel items
	Convs   []conv          `json:"c,omitempty"` // for direct messages
	Account string          `json:"a,omitempty"` // sender account, if account indexing is on
	Item    json.RawMessage `json:"i"`
}

type Pebble struct {
	logger *logger.Manager

	// dbMu guards db against concurrent Close; every access holds it for reading
	dbMu sync.RWMutex
	db   *pebble.DB

	// writeMu serializes read-modify-write mutations (idempotency checks,
	// correspondents updates, deletions)
	writeMu sync.Mutex

	stateMutex sync.Mutex
	config     Config

	wakeForgetter chan e
	stop          chan e
}

var _ history.Database = (*Pebble)(nil)

func NewPebbleDatabase(logger *logger.Manager, config Config) (*Pebble, error) {
	p := &Pebble{
		logger:        logger,
		wakeForgetter: make(chan e, 1),
		stop:          make(chan e),
	}
	p.SetConfig(config)
	if err := p.open(); err != nil {
		return nil, err
	}
	go p.cleanupLoop()
	go p.forgetLoop()
	return p, nil
}

func (p *Pebble) SetConfig(config Config) {
	p.stateMutex.Lock()
	p.config = config
	p.stateMutex.Unlock()
}

func (p *Pebble) getConfig() Config {
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()
	return p.config
}

func (p *Pebble) writeOpts() *pebble.WriteOptions {
	if p.getConfig().Sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

type pebbleLogger struct {
	logger *logger.Manager
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	l.logger.Debug("pebble", fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Errorf(format string, args ...interface{}) {
	l.logger.Error("pebble", fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Error("pebble", "fatal:", msg)
	panic(msg)
}

func (p *Pebble) open() (err error) {
	config := p.getConfig()
	if config.Path == "" {
		return errors.New("pebble history: path must be set")
	}
	cacheMB := config.CacheSizeMB
	if cacheMB <= 0 {
		cacheMB = defaultCacheSizeMB
	}
	cache := pebble.NewCache(int64(cacheMB) << 20)
	defer cache.Unref()

	p.logger.Debug("pebble", "Opening Pebble history DB at", config.Path)
	db, err := pebble.Open(config.Path, &pebble.Options{
		Cache:  cache,
		Logger: pebbleLogger{p.logger},
	})
	if err != nil {
		return fmt.Errorf("could not open pebble history database: %w", err)
	}

	schema, err := getValue(db, keySchema)
	if err == pebble.ErrNotFound {
		err = db.Set(keySchema, []byte(latestSchema), pebble.Sync)
	} else if err == nil && string(schema) != latestSchema {
		err = fmt.Errorf("incompatible pebble history schema: got %s, expected %s", schema, latestSchema)
	}
	if err != nil {
		db.Close()
		return err
	}

	p.db = db
	return nil
}

func (p *Pebble) Close() (err error) {
	p.dbMu.Lock()
	defer p.dbMu.Unlock()
	if p.db == nil {
		return nil
	}
	close(p.stop)
	err = p.db.Close()
	p.db = nil
	return
}

// withDB runs fn with the database held open, or returns errClosed.
func (p *Pebble) withDB(fn func(db *pebble.DB) error) error {
	p.dbMu.RLock()
	defer p.dbMu.RUnlock()
	if p.db == nil {
		return errClosed
	}
	return fn(p.db)
}

func (p *Pebble) logError(context string, err error) (quit bool) {
	if err != nil && err != errClosed {
		p.logger.Error("pebble", context, err.Error())
		return true
	}
	return err != nil
}

// key encoding

func appendStr(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func readStr(b []byte) (s string, rest []byte, ok bool) {
	if len(b) < 2 {
		return
	}
	n := int(binary.BigEndian.Uint16(b))
	if len(b) < 2+n {
		return
	}
	return string(b[2 : 2+n]), b[2+n:], true
}

func appendNano(b []byte, nano int64) []byte {
	if nano < 0 {
		nano = 0
	}
	return binary.BigEndian.AppendUint64(b, uint64(nano))
}

func readNano(b []byte) (nano int64, rest []byte, ok bool) {
	if len(b) < 8 {
		return
	}
	return int64(binary.BigEndian.Uint64(b)), b[8:], true
}

func recordPrefix(msgid string) []byte {
	return appendStr([]byte{pfxRecord}, msgid)
}

func recordKey(msgid, target string) []byte {
	return appendStr(recordPrefix(msgid), target)
}

// index keys end with the record key minus its prefix byte, so that they're unique
func withSuffix(b, rk []byte) []byte {
	return append(b, rk[1:]...)
}

func seqPrefix(channel string) []byte {
	return appendStr([]byte{pfxSeq}, channel)
}

func convPrefix(target, correspondent string) []byte {
	return appendStr(appendStr([]byte{pfxConv}, target), correspondent)
}

func accountPrefix(account string) []byte {
	return appendStr([]byte{pfxAccount}, account)
}

func corrKey(target, correspondent string) []byte {
	return appendStr(appendStr([]byte{pfxCorr}, target), correspondent)
}

func corrTimePrefix(target string) []byte {
	return appendStr([]byte{pfxCorrTime}, target)
}

func corrTimeKey(target string, nano int64, correspondent string) []byte {
	return appendStr(appendNano(corrTimePrefix(target), nano), correspondent)
}

func expireKey(nano int64, rk []byte) []byte {
	return withSuffix(appendNano([]byte{pfxExpire}, nano), rk)
}

// prefixEnd returns the smallest key greater than every key with the given prefix
func prefixEnd(prefix []byte) []byte {
	end := slices.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // no upper bound
}

// timeBounds computes iterator bounds within prefix for nano in (after, before),
// where zero times mean unbounded.
func timeBounds(prefix []byte, after, before time.Time) (lower, upper []byte) {
	lower = prefix
	if !after.IsZero() {
		lower = appendNano(slices.Clone(prefix), after.UnixNano()+1)
	}
	if !before.IsZero() {
		upper = appendNano(slices.Clone(prefix), before.UnixNano())
	} else {
		upper = prefixEnd(prefix)
	}
	return
}

func getValue(db *pebble.DB, key []byte) (value []byte, err error) {
	v, closer, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	value = slices.Clone(v)
	closer.Close()
	return value, nil
}

func getRecord(db *pebble.DB, rk []byte) (rec record, err error) {
	data, err := getValue(db, rk)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &rec)
	return
}

// writes

func (p *Pebble) AddChannelItem(target string, item history.Item, account string) (err error) {
	if target == "" {
		return utils.ErrInvalidParams
	}
	rec := record{
		Nano:   item.Message.Time.UnixNano(),
		Target: target,
	}
	if p.getConfig().TrackAccountMessages {
		rec.Account = account
	}
	return p.insert(recordKey(item.Message.Msgid, target), rec, &item)
}

func (p *Pebble) AddDirectMessage(sender, senderAccount, recipient, recipientAccount string, item history.Item) (err error) {
	if senderAccount == "" && recipientAccount == "" {
		return
	}
	if sender == "" || recipient == "" {
		return utils.ErrInvalidParams
	}
	rec := record{Nano: item.Message.Time.UnixNano()}
	if senderAccount != "" {
		rec.Convs = append(rec.Convs, conv{Target: senderAccount, Correspondent: recipient})
	}
	if recipientAccount != "" && sender != recipient {
		rec.Convs = append(rec.Convs, conv{Target: recipientAccount, Correspondent: sender})
	}
	if p.getConfig().TrackAccountMessages {
		rec.Account = senderAccount
	}
	return p.insert(recordKey(item.Message.Msgid, ""), rec, &item)
}

func (p *Pebble) insert(rk []byte, rec record, item *history.Item) error {
	itemData, err := history.MarshalItem(item)
	if err != nil {
		return fmt.Errorf("could not marshal item: %w", err)
	}
	rec.Item = itemData
	recData, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("could not marshal record: %w", err)
	}

	return p.withDB(func(db *pebble.DB) error {
		p.writeMu.Lock()
		defer p.writeMu.Unlock()

		// idempotency: if this (msgid, target) is already stored, do nothing
		if _, err := getValue(db, rk); err == nil {
			return nil
		} else if err != pebble.ErrNotFound {
			return err
		}

		b := db.NewBatch()
		defer b.Close()
		b.Set(rk, recData, nil)
		b.Set(expireKey(rec.Nano, rk), rk, nil)
		if rec.Target != "" {
			b.Set(withSuffix(appendNano(seqPrefix(rec.Target), rec.Nano), rk), rk, nil)
		}
		for _, c := range rec.Convs {
			b.Set(withSuffix(appendNano(convPrefix(c.Target, c.Correspondent), rec.Nano), rk), rk, nil)
			if err := updateCorrespondent(db, b, c, rec.Nano); err != nil {
				return err
			}
		}
		if rec.Account != "" {
			b.Set(withSuffix(appendNano(accountPrefix(rec.Account), rec.Nano), rk), rk, nil)
		}
		if err := b.Commit(p.writeOpts()); err != nil {
			return fmt.Errorf("could not insert history item: %w", err)
		}
		return nil
	})
}

// updateCorrespondent records nano as the latest message time between c.Target
// and c.Correspondent, if it's newer than what's stored. requires writeMu.
func updateCorrespondent(db *pebble.DB, b *pebble.Batch, c conv, nano int64) error {
	ck := corrKey(c.Target, c.Correspondent)
	old, err := getValue(db, ck)
	if err == nil {
		oldNano, _, ok := readNano(old)
		if ok && oldNano >= nano {
			return nil
		}
		if ok {
			b.Delete(corrTimeKey(c.Target, oldNano, c.Correspondent), nil)
		}
	} else if err != pebble.ErrNotFound {
		return err
	}
	b.Set(ck, appendNano(nil, nano), nil)
	b.Set(corrTimeKey(c.Target, nano, c.Correspondent), nil, nil)
	return nil
}

// deleteRecord adds deletions for a record and all its index entries to b.
// requires writeMu.
func deleteRecord(b *pebble.Batch, rk []byte, rec record) {
	b.Delete(rk, nil)
	b.Delete(expireKey(rec.Nano, rk), nil)
	if rec.Target != "" {
		b.Delete(withSuffix(appendNano(seqPrefix(rec.Target), rec.Nano), rk), nil)
	}
	for _, c := range rec.Convs {
		b.Delete(withSuffix(appendNano(convPrefix(c.Target, c.Correspondent), rec.Nano), rk), nil)
	}
	if rec.Account != "" {
		b.Delete(withSuffix(appendNano(accountPrefix(rec.Account), rec.Nano), rk), nil)
	}
}

// deleteRecordKeys deletes the records with the given keys, plus any index
// entries in `orphans` (index keys whose record may already be gone).
// if expiring is set, correspondents entries that are no newer than the
// deleted messages are removed too. requires writeMu.
func (p *Pebble) deleteRecordKeys(db *pebble.DB, rks [][]byte, orphans [][]byte, expiring bool) (count int, err error) {
	b := db.NewBatch()
	defer b.Close()
	for i, rk := range rks {
		rec, err := getRecord(db, rk)
		if err == pebble.ErrNotFound {
			if orphans != nil {
				b.Delete(orphans[i], nil)
			}
			continue
		} else if err != nil {
			return count, err
		}
		deleteRecord(b, rk, rec)
		count++
		if expiring {
			for _, c := range rec.Convs {
				ck := corrKey(c.Target, c.Correspondent)
				v, err := getValue(db, ck)
				if err != nil {
					continue
				}
				if nano, _, ok := readNano(v); ok && nano <= rec.Nano {
					b.Delete(ck, nil)
					b.Delete(corrTimeKey(c.Target, nano, c.Correspondent), nil)
				}
			}
		}
	}
	return count, b.Commit(p.writeOpts())
}

func (p *Pebble) DeleteMsgid(msgid string) (err error) {
	err = p.withDB(func(db *pebble.DB) error {
		p.writeMu.Lock()
		defer p.writeMu.Unlock()

		rks, err := scanKeys(db, recordPrefix(msgid), prefixEnd(recordPrefix(msgid)), true, 0)
		if err != nil {
			return err
		}
		if len(rks) == 0 {
			return history.ErrNotFound
		}
		_, err = p.deleteRecordKeys(db, rks, nil, false)
		if err != nil {
			return fmt.Errorf("couldn't delete msgid: %w", err)
		}
		return nil
	})
	if err == errClosed {
		err = history.ErrNotFound
	}
	return
}

// expiry

func (p *Pebble) cleanupLoop() {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("pebble",
				fmt.Sprintf("Panic in cleanup routine: %v\n%s", r, debug.Stack()))
			time.Sleep(cleanupPauseTime)
			go p.cleanupLoop()
		}
	}()

	for {
		if expireTime := p.getConfig().ExpireTime; expireTime != 0 {
			for {
				count, err := p.doCleanup(time.Now().Add(-expireTime))
				if p.logError("error during cleanup", err) || count < cleanupBatchSize {
					break
				}
			}
		}
		select {
		case <-p.stop:
			return
		case <-time.After(cleanupPauseTime):
		}
	}
}

// doCleanup deletes up to cleanupBatchSize records older than threshold.
func (p *Pebble) doCleanup(threshold time.Time) (count int, err error) {
	err = p.withDB(func(db *pebble.DB) error {
		lower := []byte{pfxExpire}
		upper := appendNano([]byte{pfxExpire}, threshold.UnixNano())
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		var rks, eks [][]byte
		for valid := iter.First(); valid && len(rks) < cleanupBatchSize; valid = iter.Next() {
			eks = append(eks, slices.Clone(iter.Key()))
			rks = append(rks, slices.Clone(iter.Value()))
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if len(rks) == 0 {
			return nil
		}

		p.writeMu.Lock()
		defer p.writeMu.Unlock()
		_, err = p.deleteRecordKeys(db, rks, eks, true)
		count = len(rks)
		if err == nil {
			p.logger.Debug("pebble", fmt.Sprintf("expired %d history records", count))
		}
		return err
	})
	return
}

// forget (GDPR deletion)

func (p *Pebble) Forget(account string) {
	if account == "" {
		return
	}
	err := p.withDB(func(db *pebble.DB) error {
		key := appendStr(appendNano([]byte{pfxForget}, time.Now().UnixNano()), account)
		return db.Set(key, nil, pebble.Sync)
	})
	if p.logError("can't enqueue forget", err) {
		return
	}
	select {
	case p.wakeForgetter <- e{}:
	default:
	}
}

func (p *Pebble) forgetLoop() {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("pebble",
				fmt.Sprintf("Panic in forget routine: %v\n%s", r, debug.Stack()))
			time.Sleep(cleanupPauseTime)
			go p.forgetLoop()
		}
	}()

	for {
		for {
			found, err := p.doForget()
			if p.logError("error processing forget", err) {
				if err == errClosed {
					return
				}
				select {
				case <-p.stop:
					return
				case <-time.After(cleanupPauseTime):
				}
			}
			if !found {
				break
			}
		}
		select {
		case <-p.stop:
			return
		case <-p.wakeForgetter:
		}
	}
}

func (p *Pebble) doForget() (found bool, err error) {
	var fk []byte
	var account string
	err = p.withDB(func(db *pebble.DB) error {
		keys, err := scanKeys(db, []byte{pfxForget}, []byte{pfxForget + 1}, true, 1)
		if err != nil || len(keys) == 0 {
			return err
		}
		fk = keys[0]
		_, rest, _ := readNano(fk[1:])
		account, _, _ = readStr(rest)
		return nil
	})
	if err != nil || fk == nil {
		return false, err
	}

	for {
		count, err := p.doForgetIteration(account)
		if err != nil {
			return true, err
		}
		if count == 0 {
			break
		}
	}
	p.logger.Debug("pebble", "forget complete for account", account)

	err = p.withDB(func(db *pebble.DB) error {
		return db.Delete(fk, pebble.Sync)
	})
	return true, err
}

func (p *Pebble) doForgetIteration(account string) (count int, err error) {
	err = p.withDB(func(db *pebble.DB) error {
		prefix := accountPrefix(account)
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return err
		}
		var rks, aks [][]byte
		for valid := iter.First(); valid && len(rks) < cleanupBatchSize; valid = iter.Next() {
			aks = append(aks, slices.Clone(iter.Key()))
			rks = append(rks, slices.Clone(iter.Value()))
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if len(rks) == 0 {
			return nil
		}
		p.writeMu.Lock()
		defer p.writeMu.Unlock()
		p.logger.Debug("pebble", fmt.Sprintf("deleting %d history records from account %s", len(rks), account))
		_, err = p.deleteRecordKeys(db, rks, aks, false)
		count = len(rks)
		return err
	})
	return
}

// reads

// scanKeys returns up to limit keys (or values, if !keys) in [lower, upper);
// limit <= 0 means no limit.
func scanKeys(db *pebble.DB, lower, upper []byte, keys bool, limit int) (results [][]byte, err error) {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	for valid := iter.First(); valid && (limit <= 0 || len(results) < limit); valid = iter.Next() {
		if keys {
			results = append(results, slices.Clone(iter.Key()))
		} else {
			results = append(results, slices.Clone(iter.Value()))
		}
	}
	return results, iter.Close()
}

// scanRange collects up to limit entries from [lower, upper), oldest first if
// ascending and newest first otherwise. fn receives each key and value.
func scanRange(db *pebble.DB, lower, upper []byte, ascending bool, limit int, fn func(k, v []byte)) error {
	if limit <= 0 || (upper != nil && bytes.Compare(lower, upper) >= 0) {
		return nil
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	count := 0
	if ascending {
		for valid := iter.First(); valid && count < limit; valid = iter.Next() {
			fn(iter.Key(), iter.Value())
			count++
		}
	} else {
		for valid := iter.Last(); valid && count < limit; valid = iter.Prev() {
			fn(iter.Key(), iter.Value())
			count++
		}
	}
	return iter.Close()
}

// lookupMsgid returns the first record stored under msgid
func lookupMsgid(db *pebble.DB, msgid string) (rec record, err error) {
	values, err := scanKeys(db, recordPrefix(msgid), prefixEnd(recordPrefix(msgid)), false, 1)
	if err != nil {
		return
	}
	if len(values) == 0 {
		err = history.ErrNotFound
		return
	}
	err = json.Unmarshal(values[0], &rec)
	return
}

func (p *Pebble) LoadMsgid(msgid string) (channel string, item history.Item, err error) {
	err = p.withDB(func(db *pebble.DB) error {
		rec, err := lookupMsgid(db, msgid)
		if err != nil {
			return err
		}
		channel = rec.Target
		return history.UnmarshalItem(rec.Item, &item)
	})
	if err == errClosed {
		err = history.ErrNotFound
	}
	return
}

func (p *Pebble) betweenTimestamps(prefix []byte, after, before, cutoff time.Time, limit int) (results []history.Item, err error) {
	after, before, ascending := history.MinMaxAsc(after, before, cutoff)
	lower, upper := timeBounds(prefix, after, before)

	err = p.withDB(func(db *pebble.DB) error {
		var rks [][]byte
		err := scanRange(db, lower, upper, ascending, limit, func(k, v []byte) {
			rks = append(rks, slices.Clone(v))
		})
		if err != nil {
			return fmt.Errorf("could not scan history: %w", err)
		}
		results = make([]history.Item, 0, len(rks))
		for _, rk := range rks {
			rec, err := getRecord(db, rk)
			if err == pebble.ErrNotFound {
				continue // deleted concurrently
			} else if err != nil {
				return fmt.Errorf("could not read history record: %w", err)
			}
			var item history.Item
			if err := history.UnmarshalItem(rec.Item, &item); err != nil {
				return fmt.Errorf("could not unmarshal history item: %w", err)
			}
			results = append(results, item)
		}
		return nil
	})
	if err == nil && !ascending {
		slices.Reverse(results)
	}
	return
}

func (p *Pebble) ListCorrespondents(cftarget string, start, end time.Time, limit int) (results []history.TargetListing, err error) {
	after, before, ascending := history.MinMaxAsc(start, end, time.Time{})
	prefix := corrTimePrefix(cftarget)
	lower, upper := timeBounds(prefix, after, before)

	err = p.withDB(func(db *pebble.DB) error {
		return scanRange(db, lower, upper, ascending, limit, func(k, v []byte) {
			nano, rest, ok := readNano(k[len(prefix):])
			if !ok {
				return
			}
			correspondent, _, ok := readStr(rest)
			if !ok {
				return
			}
			results = append(results, history.TargetListing{
				CfName: correspondent,
				Time:   time.Unix(0, nano).UTC(),
			})
		})
	})
	if err == errClosed {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("could not read correspondents: %w", err)
	}
	if !ascending {
		slices.Reverse(results)
	}
	return
}

func (p *Pebble) ListChannels(cfchannels []string) (results []history.TargetListing, err error) {
	if len(cfchannels) == 0 {
		return
	}
	err = p.withDB(func(db *pebble.DB) error {
		results = make([]history.TargetListing, 0, len(cfchannels))
		for _, chname := range cfchannels {
			prefix := seqPrefix(chname)
			err := scanRange(db, prefix, prefixEnd(prefix), false, 1, func(k, v []byte) {
				if nano, _, ok := readNano(k[len(prefix):]); ok {
					results = append(results, history.TargetListing{
						CfName: chname,
						Time:   time.Unix(0, nano).UTC(),
					})
				}
			})
			if err != nil {
				return fmt.Errorf("could not query channel listing: %w", err)
			}
		}
		return nil
	})
	if err == errClosed {
		return nil, nil
	}
	return
}

func (p *Pebble) Export(account string, writer io.Writer) {
	err := p.withDB(func(db *pebble.DB) error {
		prefix := accountPrefix(account)
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return err
		}
		defer iter.Close()
		for valid := iter.First(); valid; valid = iter.Next() {
			rec, err := getRecord(db, iter.Value())
			if err == pebble.ErrNotFound {
				continue
			} else if err != nil {
				return err
			}
			// like the SQL backends, only channel messages are exported
			if rec.Target == "" {
				continue
			}
			var item history.Item
			if err := history.UnmarshalItem(rec.Item, &item); err != nil {
				return err
			}
			item.CfCorrespondent = rec.Target
			line, err := json.Marshal(item)
			if err != nil {
				return err
			}
			writer.Write(line)
			writer.Write([]byte{'\n'})
		}
		return nil
	})
	p.logError("could not export history", err)
}

// implements history.Sequence, emulating a single history buffer (for a channel,
// a single user's DMs, or a DM conversation)
type pebbleSequence struct {
	p      *Pebble
	prefix []byte
	cutoff time.Time
}

func (p *Pebble) MakeSequence(target, correspondent string, cutoff time.Time) history.Sequence {
	var prefix []byte
	if correspondent == "" {
		prefix = seqPrefix(target)
	} else {
		prefix = convPrefix(target, correspondent)
	}
	return &pebbleSequence{p: p, prefix: prefix, cutoff: cutoff}
}

func (s *pebbleSequence) resolve(sel history.Selector) (t time.Time, err error) {
	if sel.Msgid == "" {
		return sel.Time, nil
	}
	err = s.p.withDB(func(db *pebble.DB) error {
		rec, err := lookupMsgid(db, sel.Msgid)
		if err != nil {
			return err
		}
		t = time.Unix(0, rec.Nano).UTC()
		return nil
	})
	return
}

func (s *pebbleSequence) Between(start, end history.Selector, limit int) (results []history.Item, err error) {
	startTime, err := s.resolve(start)
	if err != nil {
		if err == history.ErrNotFound || err == errClosed {
			return nil, nil
		}
		return nil, err
	}
	endTime, err := s.resolve(end)
	if err != nil {
		if err == history.ErrNotFound || err == errClosed {
			return nil, nil
		}
		return nil, err
	}
	results, err = s.p.betweenTimestamps(s.prefix, startTime, endTime, s.cutoff, limit)
	if err == errClosed {
		return nil, nil
	}
	return
}

func (s *pebbleSequence) Around(start history.Selector, limit int) (results []history.Item, err error) {
	return history.GenericAround(s, start, limit)
}

func (s *pebbleSequence) Cutoff() time.Time {
	return s.cutoff
}

func (s *pebbleSequence) Ephemeral() bool {
	return false
}
