// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ergochat/ergo/irc/history"
)

const (
	historyStream = "ERGO_HISTORY"
	// ergo.history.add.<hex(sender account)> | ergo.history.del | ergo.history.forget
	historySubjects   = "ergo.history.>"
	historyAddSubject = "ergo.history.add."
	historyDelSubject = "ergo.history.del"
	historyForgetSubj = "ergo.history.forget"

	// window within which JetStream drops republished events with the same id
	dedupWindow = 2 * time.Minute

	publishQueueSize = 16384
	publishTimeout   = 10 * time.Second
	// short timeout for stream setup: requests made before the cluster has
	// elected a meta leader hang until they time out, so retry quickly
	setupTimeout    = 3 * time.Second
	setupRetryDelay = 500 * time.Millisecond
	nakDelay        = 5 * time.Second
)

type historyOp string

const (
	opChannel historyOp = "channel"
	opDM      historyOp = "dm"
	opDelete  historyOp = "delete"
	opForget  historyOp = "forget"
)

// historyEvent is a replicated history mutation. Field use depends on Op.
type historyEvent struct {
	Op     historyOp `json:"op"`
	Origin string    `json:"origin"`

	// opChannel
	Target string `json:"target,omitempty"`
	// opChannel: sender account; opForget: account to forget
	Account string `json:"account,omitempty"`
	// opDM
	Sender           string `json:"sender,omitempty"`
	SenderAccount    string `json:"senderAccount,omitempty"`
	Recipient        string `json:"recipient,omitempty"`
	RecipientAccount string `json:"recipientAccount,omitempty"`
	// opChannel, opDM
	Item json.RawMessage `json:"item,omitempty"`
	// opDelete
	Msgid string `json:"msgid,omitempty"`
}

type outgoing struct {
	subject string
	msgID   string
	data    []byte
	// for opForget: purge this subject from the stream before publishing
	purge string
}

// ReplicatedHistory wraps a local history.Database. Writes go to the local
// database first (so the hot path never waits on the network) and are then
// published to a JetStream stream; every node consumes the stream with its own
// durable consumer and applies other nodes' writes to its local database.
// The local database must make inserts idempotent (the Pebble backend does).
type ReplicatedHistory struct {
	history.Database
	c *Cluster

	queue  chan outgoing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	stream   jetstream.Stream
	consumer jetstream.ConsumeContext
}

var _ history.Database = (*ReplicatedHistory)(nil)

// ReplicateHistory starts replicating local into the cluster. Stream setup
// happens in the background and is retried until the cluster is available,
// so this never blocks startup.
func (c *Cluster) ReplicateHistory(local history.Database) *ReplicatedHistory {
	ctx, cancel := context.WithCancel(context.Background())
	r := &ReplicatedHistory{
		Database: local,
		c:        c,
		queue:    make(chan outgoing, publishQueueSize),
		ctx:      ctx,
		cancel:   cancel,
	}
	r.wg.Add(1)
	go r.run()
	return r
}

func (r *ReplicatedHistory) run() {
	defer r.wg.Done()
	for {
		err := r.setup()
		if err == nil {
			break
		}
		r.c.logger.Warning("cluster", "history replication not ready, retrying:", err.Error())
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(setupRetryDelay):
		}
	}
	r.c.logger.Info("cluster", "history replication active")
	r.publishLoop()
}

func (r *ReplicatedHistory) setup() error {
	ctx, cancel := context.WithTimeout(r.ctx, setupTimeout)
	defer cancel()

	stream, err := r.c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        historyStream,
		Description: "ergo persistent history replication",
		Subjects:    []string{historySubjects},
		Storage:     jetstream.FileStorage,
		Replicas:    r.c.config.Replicas,
		MaxAge:      r.c.config.History.StreamMaxAge,
		Duplicates:  dedupWindow,
	})
	if err != nil {
		return fmt.Errorf("could not create history stream: %w", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "history-" + r.c.config.NodeName,
		Description:   "history replication into node " + r.c.config.NodeName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 1024,
		Replicas:      r.c.config.Replicas,
	})
	if err != nil {
		return fmt.Errorf("could not create history consumer: %w", err)
	}
	cc, err := cons.Consume(r.apply)
	if err != nil {
		return fmt.Errorf("could not consume history stream: %w", err)
	}
	r.mu.Lock()
	r.stream, r.consumer = stream, cc
	r.mu.Unlock()
	return nil
}

func (r *ReplicatedHistory) publishLoop() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case out := <-r.queue:
			r.publish(out)
		}
	}
}

// publish sends one event, retrying until it's acknowledged or we shut down.
func (r *ReplicatedHistory) publish(out outgoing) {
	for attempt := 0; ; attempt++ {
		err := r.publishOnce(out)
		if err == nil {
			return
		}
		if attempt == 0 || attempt%30 == 0 {
			r.c.logger.Warning("cluster", "could not publish history event, retrying:", err.Error())
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *ReplicatedHistory) publishOnce(out outgoing) error {
	ctx, cancel := context.WithTimeout(r.ctx, publishTimeout)
	defer cancel()
	if out.purge != "" {
		r.mu.Lock()
		stream := r.stream
		r.mu.Unlock()
		err := stream.Purge(ctx, jetstream.WithPurgeSubject(out.purge))
		if err != nil {
			return fmt.Errorf("could not purge forgotten history: %w", err)
		}
	}
	var opts []jetstream.PublishOpt
	if out.msgID != "" {
		opts = append(opts, jetstream.WithMsgID(out.msgID))
	}
	_, err := r.c.js.PublishMsg(ctx, &nats.Msg{Subject: out.subject, Data: out.data}, opts...)
	return err
}

func (r *ReplicatedHistory) enqueue(ev historyEvent, subject, msgID, purge string) {
	ev.Origin = r.c.config.NodeName
	data, err := json.Marshal(ev)
	if err != nil {
		r.c.logger.Error("cluster", "could not marshal history event:", err.Error())
		return
	}
	select {
	case r.queue <- outgoing{subject: subject, msgID: msgID, data: data, purge: purge}:
	default:
		r.c.logger.Error("cluster", "history replication queue is full, dropping event", string(ev.Op))
	}
}

// subjects are per sender account so that Forget can purge an account's
// messages from the stream; the account is hex-encoded to keep the subject valid
func addSubject(account string) string {
	if account == "" {
		return historyAddSubject + "_"
	}
	return historyAddSubject + hex.EncodeToString([]byte(account))
}

// apply handles an event from the stream.
func (r *ReplicatedHistory) apply(msg jetstream.Msg) {
	var ev historyEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		r.c.logger.Error("cluster", "dropping malformed history event:", err.Error())
		msg.Term()
		return
	}
	if ev.Origin == r.c.config.NodeName {
		msg.Ack() // already applied locally
		return
	}
	if err := r.applyEvent(&ev); err != nil {
		r.c.logger.Error("cluster", fmt.Sprintf("could not apply %s event from %s:", ev.Op, ev.Origin), err.Error())
		msg.NakWithDelay(nakDelay)
		return
	}
	msg.Ack()
}

func (r *ReplicatedHistory) applyEvent(ev *historyEvent) error {
	local := r.Database
	switch ev.Op {
	case opChannel, opDM:
		var item history.Item
		if err := history.UnmarshalItem(ev.Item, &item); err != nil {
			return err
		}
		if ev.Op == opChannel {
			return local.AddChannelItem(ev.Target, item, ev.Account)
		}
		return local.AddDirectMessage(ev.Sender, ev.SenderAccount, ev.Recipient, ev.RecipientAccount, item)
	case opDelete:
		if err := local.DeleteMsgid(ev.Msgid); err != nil && !errors.Is(err, history.ErrNotFound) {
			return err
		}
		return nil
	case opForget:
		local.Forget(ev.Account)
		return nil
	default:
		return fmt.Errorf("unknown history op %q", ev.Op)
	}
}

// history.Database writes: apply locally, then replicate

func (r *ReplicatedHistory) AddChannelItem(target string, item history.Item, account string) error {
	if err := r.Database.AddChannelItem(target, item, account); err != nil {
		return err
	}
	data, err := history.MarshalItem(&item)
	if err != nil {
		return err
	}
	r.enqueue(historyEvent{Op: opChannel, Target: target, Account: account, Item: data},
		addSubject(account), "c:"+item.Message.Msgid+":"+target, "")
	return nil
}

func (r *ReplicatedHistory) AddDirectMessage(sender, senderAccount, recipient, recipientAccount string, item history.Item) error {
	if err := r.Database.AddDirectMessage(sender, senderAccount, recipient, recipientAccount, item); err != nil {
		return err
	}
	if senderAccount == "" && recipientAccount == "" {
		return nil // not stored anywhere
	}
	data, err := history.MarshalItem(&item)
	if err != nil {
		return err
	}
	r.enqueue(historyEvent{
		Op: opDM, Sender: sender, SenderAccount: senderAccount,
		Recipient: recipient, RecipientAccount: recipientAccount, Item: data,
	}, addSubject(senderAccount), "d:"+item.Message.Msgid, "")
	return nil
}

func (r *ReplicatedHistory) DeleteMsgid(msgid string) error {
	err := r.Database.DeleteMsgid(msgid)
	if err != nil && !errors.Is(err, history.ErrNotFound) {
		return err
	}
	// replicate even if not found locally: the message may exist elsewhere
	r.enqueue(historyEvent{Op: opDelete, Msgid: msgid}, historyDelSubject, "x:"+msgid, "")
	return err
}

func (r *ReplicatedHistory) Forget(account string) {
	r.Database.Forget(account)
	if account == "" {
		return
	}
	r.enqueue(historyEvent{Op: opForget, Account: account}, historyForgetSubj, "", addSubject(account))
}

// Close stops replication and closes the local database. Events still queued
// for publishing are dropped; nodes converge again via the stream on restart
// only for events that were published, so we try to flush the queue first.
func (r *ReplicatedHistory) Close() error {
	r.mu.Lock()
	active := r.stream != nil
	r.mu.Unlock()
	deadline := time.After(5 * time.Second)
flush:
	for active && len(r.queue) > 0 {
		select {
		case <-deadline:
			break flush
		case <-time.After(50 * time.Millisecond):
		}
	}
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	if r.consumer != nil {
		r.consumer.Stop()
	}
	r.mu.Unlock()
	if n := len(r.queue); n > 0 {
		r.c.logger.Warning("cluster", fmt.Sprintf("shutting down with %d unpublished history events", n))
	}
	return r.Database.Close()
}
