/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func reconcileTestOptions(dir string) Options {
	opt := getTestOptions(dir).
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactL0OnClose(true).
		// Keep values inline so no value log is involved in these tests.
		WithValueThreshold(1 << 20)
	opt.managedTxns = true
	return opt
}

func managedSetAt(t *testing.T, db *DB, key, val string, ts uint64) {
	t.Helper()
	txn := db.NewTransactionAt(0, true)
	require.NoError(t, txn.Set([]byte(key), []byte(val)))
	require.NoError(t, txn.CommitAt(ts, nil))
}

func managedDeleteAt(t *testing.T, db *DB, key string, ts uint64) {
	t.Helper()
	txn := db.NewTransactionAt(0, true)
	require.NoError(t, txn.Delete([]byte(key)))
	require.NoError(t, txn.CommitAt(ts, nil))
}

// populateReconcileFixture writes seven physical versions across three keys:
//
//	foo: set@10, set@20, set@30, delete@40
//	bar: set@100
//	keep: set@5, set@200
func populateReconcileFixture(t *testing.T, db *DB) {
	managedSetAt(t, db, "foo", "v10", 10)
	managedSetAt(t, db, "foo", "v20", 20)
	managedSetAt(t, db, "foo", "v30", 30)
	managedDeleteAt(t, db, "foo", 40)
	managedSetAt(t, db, "bar", "v100", 100)
	managedSetAt(t, db, "keep", "v5", 5)
	managedSetAt(t, db, "keep", "v200", 200)
}

// physicalVersions returns the versions of each key that are present in the
// LSM tree, including tombstones, regardless of read timestamp.
func physicalVersions(t *testing.T, db *DB) map[string][]uint64 {
	t.Helper()
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	iopts := DefaultIteratorOptions
	iopts.AllVersions = true
	iopts.PrefetchValues = false
	itr := txn.NewIterator(iopts)
	defer itr.Close()

	out := make(map[string][]uint64)
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		out[string(item.Key())] = append(out[string(item.Key())], item.Version())
	}
	return out
}

func requireVersions(t *testing.T, db *DB, want map[string][]uint64) {
	t.Helper()
	got := physicalVersions(t, db)
	require.Equal(t, len(want), len(got), "unexpected key set: %+v", got)
	for k := range want {
		sort.Slice(got[k], func(i, j int) bool { return got[k][i] < got[k][j] })
		wantSorted := append([]uint64(nil), want[k]...)
		sort.Slice(wantSorted, func(i, j int) bool { return wantSorted[i] < wantSorted[j] })
		require.Equal(t, wantSorted, got[k], "versions of %q differ", k)
	}
}

func writeAuthorityTS(t *testing.T, path string, ts uint64) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("%d\n", ts)), 0o600))
}

func writeAuthorityRaw(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func recordPathFor(dir string) string {
	return filepath.Join(dir, discardWatermarkFname)
}

func requireRecordedWatermark(t *testing.T, dir string, want uint64) {
	t.Helper()
	ts, exists, err := loadPersistedDiscardTs(dir)
	require.NoError(t, err)
	require.True(t, exists, "durable discard-watermark record missing")
	require.Equal(t, want, ts)
}

// TestReconcileDiscardTsLifecycle covers the full lifecycle across restarts:
// advancement with physical reclamation, idempotency, rollback rejection,
// unreadable authority inputs, durable restart restore and a second advance
// that drops a covered tombstone.
func TestReconcileDiscardTsLifecycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-")
	require.NoError(t, err)
	defer removeDir(dir)

	wmPath := filepath.Join(dir, "authority.watermark")
	recordPath := recordPathFor(dir)

	// Process 1: populate the fixture and settle it into Lmax via close.
	db, err := OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	populateReconcileFixture(t, db)
	require.NoError(t, db.Close())

	// Process 2: fresh start. discardTs starts at zero, so all versions are
	// still on disk after the restart.
	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)

	require.Equal(t, uint64(0), db.orc.discardAtOrBelow())
	requireVersions(t, db, map[string][]uint64{
		"foo":  {10, 20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	})

	// A missing authority file must leave everything untouched.
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.Error(t, err)
	require.Equal(t, uint64(0), db.orc.discardAtOrBelow())
	_, err = os.Stat(recordPath)
	require.True(t, os.IsNotExist(err))

	// Advance to 25 and force reclamation.
	writeAuthorityTS(t, wmPath, 25)
	applied, err := db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(25), applied)

	// Compaction keeps every version above 25 plus the newest version at or
	// below 25 per key (NumVersionsToKeep=1 boundary rule); older superseded
	// versions below that (@10) are dropped. The tombstone@40 stays (above 25).
	wantAt25 := map[string][]uint64{
		"foo":  {20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	}
	requireVersions(t, db, wantAt25)
	requireRecordedWatermark(t, dir, 25)

	// Reads above the watermark see the live value; a read at the watermark
	// sees the retained boundary version.
	requireManagedValue(t, db, math.MaxUint64, "bar", "v100")
	requireManagedValue(t, db, math.MaxUint64, "keep", "v200")
	requireManagedValue(t, db, 35, "foo", "v30")
	requireManagedValue(t, db, 25, "foo", "v20")
	requireManagedNotFound(t, db, math.MaxUint64, "foo") // tombstone@40

	// The same watermark is not re-processed: a second call changes nothing
	// and re-running reclamation converges (same physical state).
	applied, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(25), applied)
	requireVersions(t, db, wantAt25)
	requireRecordedWatermark(t, dir, 25)

	// A rollback of the authority is rejected and changes nothing.
	writeAuthorityTS(t, wmPath, 24)
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.ErrorIs(t, err, ErrDiscardTsRegressed)
	require.Equal(t, uint64(25), db.orc.discardAtOrBelow())
	requireRecordedWatermark(t, dir, 25)
	requireVersions(t, db, wantAt25)

	// Any unreadable/ill-formed authority input changes nothing.
	for _, bad := range []string{
		"-1\n",
		"abc\n",
		"   \n",
		"18446744073709551616\n", // uint64 overflow
		"25 extra\n",
	} {
		writeAuthorityRaw(t, wmPath, bad)
		_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
		require.Error(t, err, "authority content %q must be rejected", bad)
		require.Equal(t, uint64(25), db.orc.discardAtOrBelow(),
			"in-memory watermark moved on bad input %q", bad)
		requireRecordedWatermark(t, dir, 25)
	}
	require.NoError(t, os.Remove(wmPath))
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.Error(t, err)
	require.Equal(t, uint64(25), db.orc.discardAtOrBelow())

	require.NoError(t, db.Close())

	// Process 3: after restart the applied watermark is restored from disk
	// without any reconcile call, and retained versions are intact.
	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	require.Equal(t, uint64(25), db.orc.discardAtOrBelow())
	requireVersions(t, db, wantAt25)

	// Advancing past the tombstone lets the Lmax self-compaction remove all
	// versions of foo; versions above the new watermark are preserved.
	writeAuthorityTS(t, wmPath, 40)
	applied, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(40), applied)
	// keep@5 is the newest (and only) version at or below 40, so the
	// NumVersionsToKeep boundary rule retains it; only foo is fully gone
	// because its newest covered version is a tombstone.
	requireVersions(t, db, map[string][]uint64{
		"bar":  {100},
		"keep": {5, 200},
	})
	requireManagedNotFound(t, db, math.MaxUint64, "foo")
	requireManagedNotFound(t, db, 35, "foo")
	requireManagedValue(t, db, math.MaxUint64, "bar", "v100")
	requireManagedValue(t, db, math.MaxUint64, "keep", "v200")
	requireRecordedWatermark(t, dir, 40)

	require.NoError(t, db.Close())
}

// TestReconcileDiscardTsDeferredReclaim verifies convergence when the watermark
// was advanced without reclamation (or a previous reclamation was interrupted
// by a crash): a later reconcile with the same watermark performs the physical
// cleanup, including tables still sitting in L0.
func TestReconcileDiscardTsDeferredReclaim(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-defer-")
	require.NoError(t, err)
	defer removeDir(dir)
	wmPath := filepath.Join(dir, "authority.watermark")

	db, err := OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	populateReconcileFixture(t, db)
	require.NoError(t, db.Close())

	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)

	// Advance the bookkeeping without reclaiming: nothing is dropped yet.
	writeAuthorityTS(t, wmPath, 25)
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.NoError(t, err)
	requireVersions(t, db, map[string][]uint64{
		"foo":  {10, 20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	})

	// New write lands in a memtable; flush it to L0 so reclamation must fan a
	// table down from L0 into Lmax.
	managedSetAt(t, db, "quux", "v300", 300)
	db.lock.Lock()
	oldMt := db.mt
	nmt, err := db.newMemTable()
	require.NoError(t, err)
	db.mt = nmt
	db.lock.Unlock()
	require.NoError(t, db.handleMemTableFlush(oldMt, nil))
	oldMt.DecrRef()
	require.Equal(t, 1, db.lc.levels[0].numTables())

	want := map[string][]uint64{
		"foo":  {20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
		"quux": {300},
	}
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, 0, db.lc.levels[0].numTables())
	requireVersions(t, db, want)
	requireManagedValue(t, db, math.MaxUint64, "quux", "v300")

	// Re-running converges: the watermark is not reprocessed and the physical
	// state is unchanged.
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	requireVersions(t, db, want)

	require.NoError(t, db.Close())
}

// TestReconcileDiscardTsCorruptRecord verifies the fail-closed behavior when
// the durable record of the applied watermark cannot be read.
func TestReconcileDiscardTsCorruptRecord(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-corrupt-")
	require.NoError(t, err)
	defer removeDir(dir)
	wmPath := filepath.Join(dir, "authority.watermark")
	recordPath := recordPathFor(dir)

	db, err := OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	populateReconcileFixture(t, db)
	require.NoError(t, db.Close())

	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	writeAuthorityTS(t, wmPath, 25)
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.NoError(t, err)
	requireRecordedWatermark(t, dir, 25)
	require.NoError(t, db.Close())

	// Corrupt the durable record (bad magic, correct record length).
	require.NoError(t, os.WriteFile(recordPath,
		[]byte("BAD-MAGIC-BAD-MAGIC!!"), 0o600))

	// Open stays available but conservatively discards nothing.
	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	require.Equal(t, uint64(0), db.orc.discardAtOrBelow())

	// Reconciliation refuses to act while the record is unreadable.
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.ErrorIs(t, err, ErrInvalidDiscardWatermark)
	require.Equal(t, uint64(0), db.orc.discardAtOrBelow())

	// A truncated record is rejected the same way.
	require.NoError(t, os.WriteFile(recordPath, []byte{1, 2, 3}, 0o600))
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: wmPath})
	require.ErrorIs(t, err, ErrInvalidDiscardWatermark)

	// Once the bad record is removed, reconciliation advances and reclaims
	// normally.
	require.NoError(t, os.Remove(recordPath))
	_, err = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	requireVersions(t, db, map[string][]uint64{
		"foo":  {20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	})
	requireRecordedWatermark(t, dir, 25)
	require.NoError(t, db.Close())
}

// TestReconcileDiscardTsWithLiveCompactors runs reconciliation while the
// ordinary background compactors are enabled, exercising the maintenance
// window that stops and restarts them.
func TestReconcileDiscardTsWithLiveCompactors(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-live-")
	require.NoError(t, err)
	defer removeDir(dir)
	wmPath := filepath.Join(dir, "authority.watermark")

	open := func() *DB {
		opt := getTestOptions(dir).
			// Default number of compactors (4), inline values only.
			WithNumVersionsToKeep(1).
			WithValueThreshold(1 << 20)
		opt.managedTxns = true
		db, err := OpenManaged(opt)
		require.NoError(t, err)
		return db
	}

	db := open()
	populateReconcileFixture(t, db)
	require.NoError(t, db.Close())

	db = open()
	writeAuthorityTS(t, wmPath, 25)
	applied, err := db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(25), applied)
	requireVersions(t, db, map[string][]uint64{
		"foo":  {20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	})
	require.NoError(t, db.Close())
}

// TestReconcileDiscardTsZeroWatermark verifies Reclaim at a zero watermark is
// safe, drops nothing and does not create a durable record.
func TestReconcileDiscardTsZeroWatermark(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-zero-")
	require.NoError(t, err)
	defer removeDir(dir)
	wmPath := filepath.Join(dir, "authority.watermark")

	db, err := OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	populateReconcileFixture(t, db)
	require.NoError(t, db.Close())

	db, err = OpenManaged(reconcileTestOptions(dir))
	require.NoError(t, err)
	writeAuthorityTS(t, wmPath, 0)
	applied, err := db.ReconcileDiscardTs(ReconcileDiscardTsOptions{
		WatermarkFile: wmPath,
		Reclaim:       true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), applied)
	requireVersions(t, db, map[string][]uint64{
		"foo":  {10, 20, 30, 40},
		"bar":  {100},
		"keep": {5, 200},
	})
	_, statErr := os.Stat(recordPathFor(dir))
	require.True(t, os.IsNotExist(statErr))
	require.NoError(t, db.Close())
}

// TestPersistedDiscardTsRecord covers the on-disk record encoding directly.
func TestPersistedDiscardTsRecord(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-reconcile-record-")
	require.NoError(t, err)
	defer removeDir(dir)

	ts, exists, err := loadPersistedDiscardTs(dir)
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, uint64(0), ts)

	require.NoError(t, persistDiscardTs(dir, 999))
	ts, exists, err = loadPersistedDiscardTs(dir)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(999), ts)

	// The temp file must not linger and overwrites must be atomic.
	_, err = os.Stat(filepath.Join(dir, discardWatermarkTmpFname))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, persistDiscardTs(dir, 1_000))
	ts, _, err = loadPersistedDiscardTs(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), ts)

	// A flipped byte is caught by the checksum.
	record := recordPathFor(dir)
	raw, err := os.ReadFile(record)
	require.NoError(t, err)
	raw[8] ^= 0xff
	require.NoError(t, os.WriteFile(record, raw, 0o600))
	_, _, err = loadPersistedDiscardTs(dir)
	require.ErrorIs(t, err, ErrInvalidDiscardWatermark)
}

// TestReconcileDiscardTsManagedOnly guards the managed-mode precondition.
func TestReconcileDiscardTsManagedOnly(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.Panics(t, func() {
			_, _ = db.ReconcileDiscardTs(ReconcileDiscardTsOptions{WatermarkFile: "whatever"})
		})
	})
}

func requireManagedValue(t *testing.T, db *DB, readTs uint64, key, want string) {
	t.Helper()
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	item, err := txn.Get([]byte(key))
	require.NoError(t, err)
	var got string
	require.NoError(t, item.Value(func(v []byte) error {
		got = string(v)
		return nil
	}))
	require.Equal(t, want, got)
}

func requireManagedNotFound(t *testing.T, db *DB, readTs uint64, key string) {
	t.Helper()
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	_, err := txn.Get([]byte(key))
	require.ErrorIs(t, err, ErrKeyNotFound)
}
