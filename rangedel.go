/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"math"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// rangeDelPrefix prefixes the internal LSM keys that record range tombstones.
// The full internal key (before the version suffix) is rangeDelPrefix followed
// by the start key of the deleted range; the entry value holds the end key.
// Range tombstones are ordinary entries in every other respect: they flow
// through the WAL, memtables, L0 tables and compactions like a point delete.
var rangeDelPrefix = []byte("!badger!rdel!")

// rangeTombstone describes a range deletion. Every version of every key in the
// half-open interval [start, end) with version <= tombstone.version is deleted.
// An empty start means "from the beginning"; an empty end means "to the end".
type rangeTombstone struct {
	start   []byte
	end     []byte
	version uint64
}

// rangeDelIndex is an in-memory view of the range tombstones known to the DB.
// It is appended to whenever a range delete is written to the memtable and is
// rebuilt from the LSM tree when a DB is opened.
//
// The index may conservatively retain tombstones whose LSM entries have already
// been compacted away: a tombstone only ever shadows key versions at or below
// its own version, so keeping it cannot change the result of a read or
// compaction (shadowed versions would be dropped by the same version rules
// regardless). It grows with the number of DeleteRange calls during the
// lifetime of the process and is reset on DropAll; reopening the DB rebuilds it
// from whatever tombstone entries still exist on disk.
type rangeDelIndex struct {
	sync.RWMutex
	// tombstones are sorted by start key ascending; for equal start keys,
	// higher versions come first.
	tombstones []rangeTombstone
}

func newRangeDelIndex() *rangeDelIndex {
	return &rangeDelIndex{}
}

func (idx *rangeDelIndex) add(t rangeTombstone) {
	idx.Lock()
	defer idx.Unlock()
	i := sort.Search(len(idx.tombstones), func(i int) bool {
		c := bytes.Compare(idx.tombstones[i].start, t.start)
		if c != 0 {
			return c > 0
		}
		return idx.tombstones[i].version < t.version
	})
	idx.tombstones = append(idx.tombstones, rangeTombstone{})
	copy(idx.tombstones[i+1:], idx.tombstones[i:])
	idx.tombstones[i] = t
}

// covered reports whether the given key version is shadowed by a range
// tombstone visible at readTs. Tombstones newer than readTs belong to
// transactions the snapshot cannot see yet; tombstones older than keyVersion
// predate this version of the key, which was therefore (re)written after the
// range delete and is live.
func (idx *rangeDelIndex) covered(key []byte, keyVersion, readTs uint64) bool {
	idx.RLock()
	defer idx.RUnlock()
	// First tombstone whose start is greater than key; every candidate precedes
	// it, so walk backwards from there.
	i := sort.Search(len(idx.tombstones), func(i int) bool {
		return bytes.Compare(idx.tombstones[i].start, key) > 0
	})
	for j := i - 1; j >= 0; j-- {
		t := &idx.tombstones[j]
		if t.version > readTs || t.version <= keyVersion {
			// The tombstone is invisible to this snapshot, or it is the same
			// age as (or older than) this key version. The <= is deliberate:
			// a key written in the same transaction but AFTER a DeleteRange
			// shares the tombstone's commit timestamp and must remain
			// visible (such writes are reconciled with the tombstone at
			// transaction commit time).
			continue
		}
		if len(t.end) > 0 && bytes.Compare(key, t.end) >= 0 {
			continue
		}
		return true
	}
	return false
}

// clear drops every tombstone. Used by DropAll, which physically removes the
// whole tree.
func (idx *rangeDelIndex) clear() {
	idx.Lock()
	defer idx.Unlock()
	idx.tombstones = idx.tombstones[:0]
}

// dropMarkersWithPrefix removes tombstones whose internal marker key carries
// any of the given prefixes. DropPrefix physically removes such entries from
// the LSM tree, so the index must drop them too in order to stay consistent
// with the on-disk state after a restart.
func (idx *rangeDelIndex) dropMarkersWithPrefix(prefixes [][]byte) {
	idx.Lock()
	defer idx.Unlock()
	out := idx.tombstones[:0]
	for _, t := range idx.tombstones {
		marker := make([]byte, len(rangeDelPrefix)+len(t.start))
		copy(marker, rangeDelPrefix)
		copy(marker[len(rangeDelPrefix):], t.start)
		if hasAnyPrefixes(marker, prefixes) {
			continue
		}
		out = append(out, t)
	}
	idx.tombstones = out
}

// rangeMaxUserBound is an all-0xFF byte slice longer than any legal user
// key, used as the "+infinity" key-space bound for open-ended range
// tombstones when selecting overlapping tables for compaction.
var rangeMaxUserBound = bytes.Repeat([]byte{0xff}, 65001)

// tombstoneKeyRange converts a range tombstone interval into the versioned
// keyRange used by the compaction machinery.
func tombstoneKeyRange(start, end []byte) keyRange {
	kr := keyRange{left: y.KeyWithTs(start, math.MaxUint64)}
	if len(end) == 0 {
		kr.right = y.KeyWithTs(rangeMaxUserBound, 0)
	} else {
		kr.right = y.KeyWithTs(end, 0)
	}
	return kr
}

// expandKeyRangeForMarkers widens r (a compaction range, already carrying the
// version suffix convention of getKeyRange) to cover the intervals of any
// range tombstones whose internal LSM marker key physically lives in one of
// the given tables. This is what makes compaction "follow" a range
// tombstone: the marker itself sits under the internal !badger! prefix, far
// away from the user keys it shadows, so without widening, the tables that
// hold the shadowed data would never be pulled into the same compaction.
func (idx *rangeDelIndex) expandKeyRangeForMarkers(r *keyRange, tables []*table.Table) {
	if len(tables) == 0 {
		return
	}
	lo := y.ParseKey(tables[0].Smallest())
	hi := y.ParseKey(tables[0].Biggest())
	for i := 1; i < len(tables); i++ {
		if s := y.ParseKey(tables[i].Smallest()); bytes.Compare(s, lo) < 0 {
			lo = s
		}
		if b := y.ParseKey(tables[i].Biggest()); bytes.Compare(b, hi) > 0 {
			hi = b
		}
	}

	idx.RLock()
	defer idx.RUnlock()
	for i := range idx.tombstones {
		t := &idx.tombstones[i]
		marker := rangeDelKey(t.start)
		if bytes.Compare(marker, lo) < 0 || bytes.Compare(marker, hi) > 0 {
			continue
		}
		r.extend(tombstoneKeyRange(t.start, t.end))
	}
}

// pendingRangeDel is a range deletion buffered inside an uncommitted
// transaction. start/end follow the same rules as rangeTombstone. seq is the
// transaction's operation sequence at the time of the DeleteRange call.
type pendingRangeDel struct {
	start []byte
	end   []byte
	seq   int
}

// rangeDelKey builds the internal LSM key (without the version suffix) for a
// range tombstone starting at startKey.
func rangeDelKey(startKey []byte) []byte {
	k := make([]byte, len(rangeDelPrefix)+len(startKey))
	copy(k, rangeDelPrefix)
	copy(k[len(rangeDelPrefix):], startKey)
	return k
}

// rangeCovers reports whether key falls in the half-open range [start, end).
// Empty start/end mean open bounds.
func rangeCovers(start, end, key []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func validateRangeBound(name string, key []byte) error {
	if len(key) == 0 {
		// Empty (or nil) bound means the range is open on this side.
		return nil
	}
	const maxKeySize = 65000
	if len(key) > maxKeySize {
		return exceedsSize(name, maxKeySize, key)
	}
	if bytes.HasPrefix(key, badgerPrefix) {
		return ErrInvalidKey
	}
	return nil
}

// registerRangeDelete records a just-written range tombstone entry into the
// DB index. The entry's key is expected to carry the version suffix already
// (see commitAndSend).
func (db *DB) registerRangeDelete(entry *Entry) {
	k := y.ParseKey(entry.Key) // Strip the version suffix.
	if !bytes.HasPrefix(k, rangeDelPrefix) {
		return
	}
	db.rangeDels.add(rangeTombstone{
		start:   y.SafeCopy(nil, k[len(rangeDelPrefix):]),
		end:     y.SafeCopy(nil, entry.Value),
		version: y.ParseTs(entry.Key),
	})
}

// loadRangeTombstones rebuilds the in-memory range tombstone index from the
// LSM tree (memtables and tables). Range tombstones are stored as ordinary
// internal entries, so a seek to the marker prefix across the raw iterators
// collects them. This bypasses the transaction/iterator stats path, which
// matters because it runs during Open (and would otherwise count as user
// reads).
func (db *DB) loadRangeTombstones() error {
	tables, decr := db.getMemTables()
	defer decr()

	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].sl.NewUniIterator(false))
	}
	opt := DefaultIteratorOptions
	opt.Prefix = rangeDelPrefix
	opt.InternalAccess = true
	opt.AllVersions = true
	opt.PrefetchValues = false
	iters = db.lc.appendIterators(iters, &opt)
	if len(iters) == 0 {
		// Empty (possibly read-only) DB: no memtables and no tables
		// intersecting the marker key space.
		return nil
	}
	mi := table.NewMergeIterator(iters, false)
	defer mi.Close()

	mi.Seek(y.KeyWithTs(rangeDelPrefix, math.MaxUint64))
	for ; mi.Valid(); mi.Next() {
		k := y.ParseKey(mi.Key())
		if !bytes.HasPrefix(k, rangeDelPrefix) {
			// The markers form one contiguous key cluster; leaving it
			// means there is nothing left to collect.
			break
		}
		vs := mi.Value()
		db.rangeDels.add(rangeTombstone{
			start:   y.SafeCopy(nil, k[len(rangeDelPrefix):]),
			end:     y.SafeCopy(nil, vs.Value),
			version: y.ParseTs(mi.Key()),
		})
	}
	return nil
}

// DeleteRange deletes all the keys in the half-open key range
// [startKey, endKey). It is a convenience wrapper which runs the deletion in
// its own transaction; use Txn.DeleteRange to batch range deletions with other
// writes.
//
// The deletion is recorded as a range tombstone: the call returns as soon as
// the tombstone is committed, without traversing or rewriting the keys in the
// range. Reads and iterations started afterwards no longer see the keys, while
// the space they occupy is reclaimed gradually by background compactions and
// value-log GC, exactly as with point deletes. Keys written after the
// deletion (with a newer version) are not affected.
//
// startKey/endKey may be nil or empty to leave the range open on that side.
// DeleteRange is not supported with managed transactions; managed users should
// run Txn.DeleteRange inside a transaction committed at the desired timestamp.
func (db *DB) DeleteRange(startKey, endKey []byte) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	return db.Update(func(txn *Txn) error {
		return txn.DeleteRange(startKey, endKey)
	})
}
