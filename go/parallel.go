// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

// shardsPerWorker is how many shards a scan of more than one worker asks
// Split for per worker. Shards of equal bytes can take unequal time (keys
// differ in length and shape, and fn's work per object varies); a worker
// that finishes a shard takes the next one instead of waiting, so the
// scan's tail is bounded by the slowest shard still running, not by a
// fixed share of the work: Split cuts only at row groups, and one shard
// can cost more than others. A shard costs a split boundary, a part
// schema check and its own SeekParquet scan buffers,
// released when it ends. A plan with
// fewer row groups has fewer shards. A var so that tests can vary it.
var shardsPerWorker = 8

// scanShards is the most shards a scan of workers workers asks Split for.
func scanShards(workers int) int {
	if workers == 1 {
		return 1
	}
	return workers * min(shardsPerWorker, math.MaxInt/workers)
}

// ScanParallel scans the plan with at most workers concurrent callbacks.
// Workers <= 0 means one worker and one shard; otherwise up to eight queued
// shards per worker balance the work. Batches follow dataset order inside
// each shard; callbacks across shards are unordered and must be concurrency safe.
// Borrowing and Close restrictions are those of Plan.Scan. Equal-key groups
// stay intact. A complete scan also checks the key order across shard boundaries.
//
// Stop cancels and joins workers and returns success, unless a real error or
// external cancellation occurred. Early termination does not check all keys.
// Other callback errors retain identity. st is reset and includes splitting
// and all scans, including failures; add planning statistics separately.
func (plan *Plan) ScanParallel(ctx context.Context, workers int, st *ReadStats, fn func(*Batch) error) error {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if fn == nil {
		return invalidRequest("a nil batch callback")
	}
	err := plan.scanParallel(ctx, workers, st, true, func() shardScan {
		return func(ctx context.Context, shard *Plan, st *ReadStats, keys *shardKeys) error {
			var failed error
			err := shard.Scan(ctx, st, func(b *Batch) error {
				keys.noteBatch(b)
				failed = fn(b)
				return failed
			})
			if failed != nil {
				return &shardCallbackError{failed}
			}
			return err
		}
	})
	if err == Stop {
		return nil
	}
	return err
}

// shardScan and scanParallel are the queued driver from swath reader/scan.go.
// A worker owns its scan state; each shard owns statistics and boundary keys.
type shardScan func(context.Context, *Plan, *ReadStats, *shardKeys) error

func (plan *Plan) scanParallel(ctx context.Context, workers int, stats *ReadStats, checkKeys bool, worker func() shardScan) (err error) {
	rctx, err := plan.r.begin(ctx)
	if err != nil {
		return err
	}
	defer rctx.done(ctx, &err, stats)
	workers = max(workers, 1)
	var st ReadStats
	shards, err := plan.split(rctx, scanShards(workers), &st, checkKeys)
	if stats != nil {
		stats.Add(st)
	}
	if err != nil {
		return err
	}
	parent := rctx
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var first error
	var stopped bool
	var mu sync.Mutex
	shardStats := make([]ReadStats, len(shards))
	keys := make([]shardKeys, len(shards))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(workers, len(shards)) {
		wg.Go(func() {
			scanShard := worker()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(shards) || ctx.Err() != nil {
					return
				}
				if e := scanShard(ctx, shards[i], &shardStats[i], &keys[i]); e != nil {
					callback := false
					if failed, ok := e.(*shardCallbackError); ok {
						e, callback = failed.err, true
					}
					mu.Lock()
					if e == Stop {
						stopped = true
					} else if first == nil && !(ctx.Err() != nil && errors.Is(e, context.Canceled) && !callback) {
						first = e
					}
					cancel()
					mu.Unlock()
					return
				}
			}
		})
	}
	wg.Wait()
	for _, st := range shardStats {
		if stats != nil {
			stats.Add(st)
		}
	}
	if first != nil {
		return first
	}
	if err := parent.Err(); err != nil {
		return err
	}
	if stopped {
		return Stop
	}
	if checkKeys {
		return shardOrder(keys)
	}
	return nil
}

// shardKeys is the first and last key a shard handed on, owned copies.
type shardKeys struct {
	first, last []byte
	seen        bool
}

// note records first as the shard's first key, unless it has one, and
// last as its last. It copies both: a borrowed key is valid only during
// its batch, and an owning one may be changed by the callback.
func (k *shardKeys) note(first, last []byte) {
	if !k.seen {
		k.first, k.seen = append(k.first[:0], first...), true
	}
	k.last = append(k.last[:0], last...)
}

// noteBatch notes the keys of a batch of b.Len() > 0 rows.
func (k *shardKeys) noteBatch(b *Batch) { k.note(b.Key(0), b.Key(b.Len()-1)) }

// shardOrder checks the keys across the boundaries of shards that each
// succeeded, in plan order. Each shard's scan checks the order of the key
// pages it decodes against each other, and the module checks each row
// group's first key against its neighbours' first keys, but nothing
// checks a shard's first key against the last of the shard before it,
// which the one scan of an unsplit plan does. Shards begin at row groups,
// and with keys that may repeat Split keeps a group of equal keys inside
// a part in one shard, so the first key of a shard must be above the last
// of the one before it; an equal key is a duplicate of a unique key or a
// group crossing parts, both of which an unsplit scan refuses. The
// refusal is the module's key_order, its Key the shard's first key, at no
// part or row group: the reader does not see where a shard begins.
func shardOrder(keys []shardKeys) error {
	var last []byte
	for i := range keys {
		k := &keys[i]
		if !k.seen {
			continue
		}
		if last != nil && bytes.Compare(last, k.first) >= 0 {
			return &RefusalError{Code: CodeKeyOrder, RowGroup: -1, Row: -1, Key: bytes.Clone(k.first),
				Detail: "a shard's first key breaks the declared key order with the last key of the shard before it"}
		}
		last = k.last
	}
	return nil
}

// Distinguish callback cancellation errors from worker cancellation caused by
// another shard's Stop, preserving the callback's identity in either race.
type shardCallbackError struct{ err error }

func (e *shardCallbackError) Error() string { return e.err.Error() }
