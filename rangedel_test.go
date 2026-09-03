/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/stretchr/testify/require"
)

func rangeTestKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%03d", i))
}

func keyExists(t *testing.T, db *DB, key []byte) bool {
	t.Helper()
	found := false
	err := db.View(func(txn *Txn) error {
		item, err := txn.Get(key)
		if err == ErrKeyNotFound {
			return nil
		}
		require.NoError(t, err)
		found = true
		return item.Value(func(v []byte) error { return nil })
	})
	require.NoError(t, err)
	return found
}

func iterKeys(t *testing.T, db *DB, opts IteratorOptions) [][]byte {
	t.Helper()
	var keys [][]byte
	err := db.View(func(txn *Txn) error {
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, y.SafeCopy(nil, it.Item().Key()))
		}
		return nil
	})
	require.NoError(t, err)
	return keys
}

func setupRangeKeys(t *testing.T, db *DB, n int) {
	t.Helper()
	err := db.Update(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			if err := txn.Set(rangeTestKey(i), []byte("val")); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDeleteRangeBasic(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 100)

		require.NoError(t, db.DeleteRange(rangeTestKey(20), rangeTestKey(80)))

		// Point lookups inside the range are gone; outside survive.
		for i := 0; i < 100; i++ {
			exists := keyExists(t, db, rangeTestKey(i))
			if i >= 20 && i < 80 {
				require.False(t, exists, "key %d should be deleted", i)
			} else {
				require.True(t, exists, "key %d should survive", i)
			}
		}

		// Forward iteration skips the range, and boundary keys are correct
		// (half-open: the end key survives).
		keys := iterKeys(t, db, DefaultIteratorOptions)
		require.Len(t, keys, 40)
		require.True(t, bytes.Equal(keys[0], rangeTestKey(0)))
		require.True(t, bytes.Equal(keys[19], rangeTestKey(19)))
		require.True(t, bytes.Equal(keys[20], rangeTestKey(80)))
		require.True(t, bytes.Equal(keys[39], rangeTestKey(99)))

		// Reverse iteration skips the range as well.
		opt := DefaultIteratorOptions
		opt.Reverse = true
		rkeys := iterKeys(t, db, opt)
		require.Len(t, rkeys, 40)
		require.True(t, bytes.Equal(rkeys[0], rangeTestKey(99)))
		require.True(t, bytes.Equal(rkeys[19], rangeTestKey(80)))
		require.True(t, bytes.Equal(rkeys[20], rangeTestKey(19)))

		// AllVersions iteration must not leak shadowed versions either.
		opt = DefaultIteratorOptions
		opt.AllVersions = true
		require.Len(t, iterKeys(t, db, opt), 40)
	})
}

func TestDeleteRangeOpenBounds(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 100)

		// Open start: everything before key-050 is gone.
		require.NoError(t, db.DeleteRange(nil, rangeTestKey(50)))
		for i := 0; i < 100; i++ {
			exists := keyExists(t, db, rangeTestKey(i))
			if i < 50 {
				require.False(t, exists, "key %d should be deleted", i)
			} else {
				require.True(t, exists, "key %d should survive", i)
			}
		}

		// Open end: key-070 and onwards is gone.
		require.NoError(t, db.DeleteRange(rangeTestKey(70), nil))
		for i := 50; i < 100; i++ {
			exists := keyExists(t, db, rangeTestKey(i))
			if i >= 70 {
				require.False(t, exists, "key %d should be deleted", i)
			} else {
				require.True(t, exists, "key %d should survive", i)
			}
		}
	})
}

func TestDeleteRangeEverything(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 50)
		require.NoError(t, db.DeleteRange(nil, nil))
		require.Len(t, iterKeys(t, db, DefaultIteratorOptions), 0)
		require.False(t, keyExists(t, db, rangeTestKey(0)))
	})
}

func TestDeleteRangeSameTransaction(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 20)

		err := db.Update(func(txn *Txn) error {
			if err := txn.DeleteRange(rangeTestKey(5), rangeTestKey(15)); err != nil {
				return err
			}
			// Own delete is visible to own reads.
			if _, err := txn.Get(rangeTestKey(6)); err != ErrKeyNotFound {
				return err
			}
			// Writing a key inside the range after deleting the range
			// re-creates it for this transaction.
			if err := txn.Set(rangeTestKey(6), []byte("newval")); err != nil {
				return err
			}
			item, err := txn.Get(rangeTestKey(6))
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				require.True(t, bytes.Equal(v, []byte("newval")))
				return nil
			})
		})
		require.NoError(t, err)

		// After commit: key-006 holds the new value; the rest of the range is
		// gone, and the end boundary survives.
		require.True(t, keyExists(t, db, rangeTestKey(6)))
		require.False(t, keyExists(t, db, rangeTestKey(7)))
		require.True(t, keyExists(t, db, rangeTestKey(15)))

		// Write-then-delete in the same transaction: the later range delete
		// must win (mirroring Set-then-Delete for a point key).
		err = db.Update(func(txn *Txn) error {
			if err := txn.Set(rangeTestKey(16), []byte("x")); err != nil {
				return err
			}
			return txn.DeleteRange(rangeTestKey(15), rangeTestKey(17))
		})
		require.NoError(t, err)
		require.False(t, keyExists(t, db, rangeTestKey(16)))
	})
}

func TestDeleteRangeSnapshotIsolation(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 20)

		// Open a long-lived read snapshot.
		txn := db.NewTransaction(false)
		defer txn.Discard()

		require.NoError(t, db.DeleteRange(rangeTestKey(0), rangeTestKey(20)))

		// The old snapshot still observes the data.
		item, err := txn.Get(rangeTestKey(10))
		require.NoError(t, err)
		require.NotNil(t, item)

		// A fresh snapshot does not.
		require.False(t, keyExists(t, db, rangeTestKey(10)))
	})
}

func TestDeleteRangeRewriteAfterDelete(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k1"), []byte("old"))
		}))

		// Delete the range covering k1, then re-create k1.
		require.NoError(t, db.DeleteRange([]byte("k0"), []byte("k2")))
		require.False(t, keyExists(t, db, []byte("k1")))

		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k1"), []byte("new"))
		}))

		var val []byte
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte("k1"))
			require.NoError(t, err)
			return item.Value(func(v []byte) error {
				val = append(val, v...)
				return nil
			})
		}))
		require.True(t, bytes.Equal(val, []byte("new")))
	})
}

func TestDeleteRangeValidation(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Read-only transaction cannot delete.
		err := db.View(func(txn *Txn) error {
			return txn.DeleteRange([]byte("a"), []byte("b"))
		})
		require.ErrorIs(t, err, ErrReadOnlyTxn)

		err = db.Update(func(txn *Txn) error {
			// start >= end is rejected.
			if err := txn.DeleteRange([]byte("z"), []byte("a")); !errors.Is(err, ErrInvalidRequest) {
				return err
			}
			if err := txn.DeleteRange([]byte("a"), []byte("a")); !errors.Is(err, ErrInvalidRequest) {
				return err
			}
			// Reserved prefix is rejected for bounds.
			if err := txn.DeleteRange(badgerPrefix, nil); !errors.Is(err, ErrInvalidKey) {
				return err
			}
			return nil
		})
		require.NoError(t, err)
	})
}

func TestDeleteRangeWriteBatch(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		for i := 0; i < 30; i++ {
			require.NoError(t, wb.Set(rangeTestKey(i), []byte("val")))
		}
		require.NoError(t, wb.DeleteRange(rangeTestKey(10), rangeTestKey(20)))
		require.NoError(t, wb.Flush())

		for i := 0; i < 30; i++ {
			exists := keyExists(t, db, rangeTestKey(i))
			if i >= 10 && i < 20 {
				require.False(t, exists, "key %d should be deleted", i)
			} else {
				require.True(t, exists, "key %d should survive", i)
			}
		}
	})
}

func TestDeleteRangePrefixIterator(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			for _, k := range []string{"aprefix-a", "aprefix-b", "aprefix-c", "bprefix-a"} {
				if err := txn.Set([]byte(k), []byte("v")); err != nil {
					return err
				}
			}
			return nil
		}))
		require.NoError(t, db.DeleteRange([]byte("aprefix-b"), []byte("bprefix")))

		opt := DefaultIteratorOptions
		opt.Prefix = []byte("aprefix-")
		var got []string
		for _, k := range iterKeys(t, db, opt) {
			got = append(got, string(k))
		}
		require.Equal(t, []string{"aprefix-a"}, got)
	})
}

func TestDeleteRangeReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	setupRangeKeys(t, db, 100)
	require.NoError(t, db.DeleteRange(rangeTestKey(20), rangeTestKey(80)))
	// The range tombstone marker is kept inline in the LSM tree, never in
	// the value log.
	require.NoError(t, db.View(func(txn *Txn) error {
		iopt := DefaultIteratorOptions
		iopt.Prefix = rangeDelPrefix
		iopt.InternalAccess = true
		it := txn.NewIterator(iopt)
		defer it.Close()
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
			item := it.Item()
			require.Zero(t, item.meta&bitValuePointer)
			require.NotZero(t, item.meta&bitRangeDelete)
		}
		require.Equal(t, 1, count)
		return nil
	}))
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for i := 0; i < 100; i++ {
		exists := keyExists(t, db, rangeTestKey(i))
		if i >= 20 && i < 80 {
			require.False(t, exists, "key %d should stay deleted after reopen", i)
		} else {
			require.True(t, exists, "key %d should survive reopen", i)
		}
	}
}

// physicalRangeCount counts raw, non-internal keys physically present in
// memtables and SST tables (all versions), bypassing the read-side range
// tombstone filter. It is used to assert that compaction actually reclaims the
// shadowed data, not just that reads hide it.
func physicalRangeCount(t *testing.T, db *DB, start, end []byte) int {
	t.Helper()
	tables, decr := db.getMemTables()
	defer decr()

	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].sl.NewUniIterator(false))
	}
	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.InternalAccess = true
	iters = db.lc.appendIterators(iters, &opt)
	mi := table.NewMergeIterator(iters, false)
	defer mi.Close()

	count := 0
	for mi.Rewind(); mi.Valid(); mi.Next() {
		k := y.ParseKey(mi.Key())
		if bytes.HasPrefix(k, badgerPrefix) {
			continue
		}
		if bytes.Compare(k, start) >= 0 && (len(end) == 0 || bytes.Compare(k, end) < 0) {
			count++
		}
	}
	return count
}

func reopenDB(t *testing.T, opt Options) *DB {
	t.Helper()
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

// drainL0Range forces L0 -> base level compactions until L0 is empty, the same
// way DropPrefix drives compactions (forced adjusted score, ignoring the
// natural L0 table-count threshold).
func drainL0Range(t *testing.T, db *DB) {
	t.Helper()
	for db.lc.levels[0].numTables() > 0 {
		p := compactionPriority{level: 0, score: 1.74, adjusted: 1.74, t: db.lc.levelTargets()}
		err := db.lc.doCompact(176, p)
		if err == errFillTables {
			break
		}
		require.NoError(t, err)
	}
}

func TestDeleteRangeCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)

	db := reopenDB(t, opt)
	setupRangeKeys(t, db, 100)
	require.NoError(t, db.Close())

	db = reopenDB(t, opt)
	// Consolidate everything onto the last level.
	drainL0Range(t, db)
	require.Equal(t, 100, physicalRangeCount(t, db, rangeTestKey(0), rangeTestKey(100)))

	// Delete a range, then flush it to L0 by closing and reopening.
	require.NoError(t, db.DeleteRange(rangeTestKey(20), rangeTestKey(80)))
	require.NoError(t, db.Close())

	db = reopenDB(t, opt)
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, 60, physicalRangeCount(t, db, rangeTestKey(20), rangeTestKey(80)),
		"data is still physical before the tombstone compacts down")

	// Compacting the tombstone (L0) with the data level must drop shadowed
	// keys from the output tables.
	drainL0Range(t, db)
	require.Equal(t, 0, physicalRangeCount(t, db, rangeTestKey(20), rangeTestKey(80)),
		"shadowed keys must be physically dropped by compaction")
	require.Equal(t, 40, physicalRangeCount(t, db, rangeTestKey(0), rangeTestKey(100)),
		"keys outside the range must remain")

	for i := 0; i < 100; i++ {
		exists := keyExists(t, db, rangeTestKey(i))
		if i >= 20 && i < 80 {
			require.False(t, exists, "key %d deleted", i)
		} else {
			require.True(t, exists, "key %d alive", i)
		}
	}

	// Reopen once more: the rebuilt index must give the same answer.
	require.NoError(t, db.Close())
	db = reopenDB(t, opt)
	for i := 0; i < 100; i++ {
		exists := keyExists(t, db, rangeTestKey(i))
		if i >= 20 && i < 80 {
			require.False(t, exists, "key %d deleted after reopen", i)
		} else {
			require.True(t, exists, "key %d alive after reopen", i)
		}
	}
}

func TestDeleteRangeInteractsWithDeleteAndDropPrefix(t *testing.T) {
	// Point deletes and DropPrefix must keep working alongside range deletes.
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setupRangeKeys(t, db, 30)

		// Point delete still works.
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete(rangeTestKey(5))
		}))
		require.False(t, keyExists(t, db, rangeTestKey(5)))

		// Range delete removes key-010..key-019.
		require.NoError(t, db.DeleteRange(rangeTestKey(10), rangeTestKey(20)))

		// DropPrefix removes key-010..key-019 as well (same interval) and
		// must not resurrect anything.
		require.NoError(t, db.DropPrefix([]byte("key-01")))

		for i := 0; i < 30; i++ {
			exists := keyExists(t, db, rangeTestKey(i))
			switch {
			case i == 5:
				// Point-deleted.
				require.False(t, exists)
			case i >= 10 && i < 20:
				// Covered by both the range delete and DropPrefix.
				require.False(t, exists)
			default:
				require.True(t, exists, "key %d should survive", i)
			}
		}
	})
}
