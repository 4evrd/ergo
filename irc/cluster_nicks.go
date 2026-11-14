// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package irc

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ergochat/ergo/irc/cluster"
)

var errNickUnavailable = errors.New("nickname registry is temporarily unavailable")

const nickStripes = 256

// clusterNicks ties the local ClientManager to the cluster nick registry.
//
// All claim/release operations on a key (see clusterNickKey), together with
// the matching change to the local nick maps, happen under that key's stripe
// lock. So whenever this node's claim on a key is examined, the local maps
// agree with it: if this node owns the claim, whichever local client holds
// the key (if any) is the rightful holder.
type clusterNicks struct {
	server   *Server
	registry atomic.Pointer[cluster.Nicks]
	stripes  [nickStripes]sync.Mutex

	stop chan struct{}
	wg   sync.WaitGroup
}

func newClusterNicks(server *Server, registry *cluster.Nicks) *clusterNicks {
	cn := &clusterNicks{server: server, stop: make(chan struct{})}
	cn.registry.Store(registry)
	return cn
}

// clusterNickKey is the registry key for a casefolded nick. Locally two nicks
// collide if their casefolds or their skeletons match; the skeleton of the
// casefold covers both (under ASCII casemappings Skeleton is the identity, so
// the key is just the casefold).
func clusterNickKey(cfnick string) string {
	if !validSkeleton(cfnick) {
		return cfnick
	}
	if key, err := Skeleton(cfnick); err == nil {
		return key
	}
	return cfnick
}

func (cn *clusterNicks) reg() *cluster.Nicks {
	return cn.registry.Load()
}

func stripeIndex(skeleton string) int {
	h := fnv.New32a()
	h.Write([]byte(skeleton))
	return int(h.Sum32() % nickStripes)
}

func validSkeleton(skeleton string) bool {
	return skeleton != "" && skeleton != "*"
}

// lock locks the stripes for the given skeletons, in a consistent order
func (cn *clusterNicks) lock(skeletons ...string) (unlock func()) {
	var idx []int
	for _, sk := range skeletons {
		if validSkeleton(sk) {
			idx = append(idx, stripeIndex(sk))
		}
	}
	slices.Sort(idx)
	idx = slices.Compact(idx)
	for _, i := range idx {
		cn.stripes[i].Lock()
	}
	return func() {
		for j := len(idx) - 1; j >= 0; j-- {
			cn.stripes[idx[j]].Unlock()
		}
	}
}

// claim maps registry errors onto nick errors. lenient allows the nick
// without a claim when the registry is unreachable; reconciliation claims it
// (or renames the client) later.
func (cn *clusterNicks) claim(skeleton, nick, account string, lenient bool) (acquired bool, err error) {
	acquired, err = cn.reg().Claim(skeleton, cluster.NickClaim{Nick: nick, Account: account})
	switch {
	case err == nil:
		return acquired, nil
	case errors.Is(err, cluster.ErrNickInUse):
		return false, errNicknameInUse
	case lenient:
		cn.server.logger.Warning("cluster", "nick registry unavailable, allowing nick unclaimed:", nick, err.Error())
		return false, nil
	default:
		cn.server.logger.Warning("cluster", "nick registry unavailable:", err.Error())
		return false, errNickUnavailable
	}
}

func (cn *clusterNicks) release(skeleton string) {
	if !validSkeleton(skeleton) {
		return
	}
	if err := cn.reg().Release(skeleton); err != nil {
		// a stale claim is cleaned up by the next reconciliation
		cn.server.logger.Warning("cluster", "could not release nick claim:", err.Error())
	}
}

// start reconciles once (clearing claims left over from a previous run of
// this node), then periodically in the background
func (cn *clusterNicks) start(interval time.Duration) {
	cn.reconcile()
	cn.wg.Add(1)
	go func() {
		defer cn.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-cn.stop:
				return
			case <-ticker.C:
				cn.reconcile()
			}
		}
	}()
}

func (cn *clusterNicks) close() {
	close(cn.stop)
	cn.wg.Wait()
	cn.reg().Close()
}

// reconcile brings the registry in line with this node's clients: it drops
// claims no local client holds (e.g. left over from a crash), re-claims
// nicks whose claims are missing, and renames local clients whose nick was
// taken by another node while this node was unreachable.
func (cn *clusterNicks) reconcile() (released, claimed, renamed int) {
	if !cn.reg().Ready() {
		return
	}
	owned, err := cn.reg().Owned()
	if err != nil {
		cn.server.logger.Warning("cluster", "nick reconciliation failed:", err.Error())
		return
	}
	ownedSet := make(map[string]struct{}, len(owned))
	for _, sk := range owned {
		ownedSet[sk] = struct{}{}
		unlock := cn.lock(sk)
		if cn.server.clients.getByClusterKey(sk) == nil {
			cn.release(sk)
			released++
		}
		unlock()
	}

	var conflicts []*Client
	for sk, client := range cn.server.clients.clusterKeySnapshot() {
		if _, ok := ownedSet[sk]; ok {
			continue
		}
		unlock := cn.lock(sk)
		if cn.server.clients.getByClusterKey(sk) == client {
			_, err := cn.reg().Claim(sk, cluster.NickClaim{Nick: client.Nick(), Account: client.AccountName()})
			if errors.Is(err, cluster.ErrNickInUse) {
				conflicts = append(conflicts, client)
			} else if err != nil {
				cn.server.logger.Warning("cluster", "could not re-claim nick:", err.Error())
			} else {
				claimed++
			}
		}
		unlock()
	}

	// renaming takes the stripe locks itself, so do it after releasing them
	for _, client := range conflicts {
		nick := client.Nick()
		cn.server.logger.Info("cluster", fmt.Sprintf("nick %s was claimed by another node; renaming local client", nick))
		cn.server.RandomlyRename(client)
		renamed++
	}
	if released+claimed+renamed != 0 {
		cn.server.logger.Debug("cluster", fmt.Sprintf("nick reconciliation: released %d, claimed %d, renamed %d", released, claimed, renamed))
	}
	return
}
