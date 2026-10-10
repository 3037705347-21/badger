/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---- small helpers ---------------------------------------------------------

func flattenTestOptions(dir string) Options {
	opt := DefaultOptions(dir).
		WithSyncWrites(false).
		WithLoggingLevel(WARNING).
		WithNumCompactors(0).
		WithNumVersionsToKeep(1)
	// Managed timestamps let tests read hand-built tables (version=1) at a high
	// read timestamp, mirroring the setup in TestCompaction.
	opt.managedTxns = true
	return opt
}

// refreshLevelSizes recomputes totalSize/totalStaleSize (and re-sorts) on every
// populated level. createAndOpen appends tables directly without touching size
// accounting, but Flatten drives its loop off level sizes.
func refreshLevelSizes(t *testing.T, db *DB) {
	t.Helper()
	for _, l := range db.lc.levels {
		l.RLock()
		cp := append(l.tables[:0:0], l.tables...)
		l.RUnlock()
		if len(cp) > 0 {
			l.initTables(cp)
		}
	}
}

// buildLevelTree places one table per band at level 1+i. The bands must be
// mutually disjoint key ranges (levels >= 1 require non-overlapping tables).
func buildLevelTree(t *testing.T, db *DB, bands [][]string) {
	t.Helper()
	for level, band := range bands {
		kvs := make([]keyValVersion, 0, len(band))
		for _, k := range band {
			kvs = append(kvs, keyValVersion{key: k, val: "val-" + k, version: 1})
		}
		createAndOpen(db, kvs, level+1)
	}
	refreshLevelSizes(t, db)
}

func bandKeys(prefix byte, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%c-%04d", prefix, i)
	}
	return out
}

func nonEmptyLevels(db *DB) []int {
	var out []int
	for i, l := range db.lc.levels {
		if l.numTables() > 0 {
			out = append(out, i)
		}
	}
	return out
}

// readAllKeys returns key->value for the latest visible version of every key.
// Tests run in managed mode, where View reads at MaxUint64.
func readAllKeys(t *testing.T, db *DB) map[string]string {
	t.Helper()
	out := make(map[string]string)
	opt := DefaultIteratorOptions
	opt.InternalAccess = true
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			out[k] = string(v)
		}
		return nil
	}))
	return out
}

func countDoneLedgers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		name := e.Name()
		if len(name) > len(flattenLedgerFilename)+6 &&
			name[:len(flattenLedgerFilename)] == flattenLedgerFilename &&
			hasSuffix(name, "-DONE") {
			out = append(out, name)
		}
	}
	return out
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

// replayLedgerFile opens an arbitrary ledger-ish path read-only and replays it.
func replayLedgerFile(t *testing.T, path string) *flattenLedger {
	t.Helper()
	fp, err := os.Open(path)
	require.NoError(t, err)
	lg := &flattenLedger{
		fp:            fp,
		prepared:      make(map[uint64]*flattenBatch),
		knownTableIDs: make(map[uint64]struct{}),
	}
	_, err = lg.replay(false)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	return lg
}

// installLedgerHook installs the global test hook and returns a snapshot
// accessor for the recorded frames together with a setter for the per-record
// callback. The accessor (not the underlying slice) is returned because hook
// goroutines append under a mutex and append may reallocate the header.
func installLedgerHook(t *testing.T) (func() []flattenLedgerRecord, func(func(flattenLedgerRecord) error)) {
	t.Helper()
	var mu sync.Mutex
	var recs []flattenLedgerRecord
	snapshot := func() []flattenLedgerRecord {
		mu.Lock()
		defer mu.Unlock()
		return append([]flattenLedgerRecord(nil), recs...)
	}
	set := func(fn func(flattenLedgerRecord) error) {
		flattenLedgerTestHook = func(rec flattenLedgerRecord) error {
			mu.Lock()
			recs = append(recs, rec)
			mu.Unlock()
			if fn != nil {
				return fn(rec)
			}
			return nil
		}
	}
	set(nil)
	t.Cleanup(func() { flattenLedgerTestHook = nil })
	return snapshot, set
}

// rotateActiveMemtable forces the current head memtable into the flush queue
// and waits until the flusher persisted it as an L0 table.
func rotateActiveMemtable(t *testing.T, db *DB) {
	t.Helper()
	db.lock.Lock()
	mt := db.mt
	require.NotNil(t, mt)
	db.flushChan <- mt
	db.imm = append(db.imm, mt)
	newmt, err := db.newMemTable()
	require.NoError(t, err)
	db.mt = newmt
	db.lock.Unlock()

	require.Eventually(t, func() bool {
		db.lock.RLock()
		immN := len(db.imm)
		db.lock.RUnlock()
		return immN == 0 && len(db.flushChan) == 0 && db.lc.levels[0].numTables() > 0
	}, 15*time.Second, 5*time.Millisecond)
}

func tm(level int) TableManifest { return TableManifest{Level: uint8(level)} }

// ---- 1. framing / replay / torn tail --------------------------------------

func TestFlattenLedgerFraming(t *testing.T) {
	dir := t.TempDir()

	lg, err := beginFlattenJob(dir, 6, 100)
	require.NoError(t, err)
	seq, err := lg.prepareBatch(5, 6,
		[]ledgerTableRef{{ID: 10, Level: 5}, {ID: 11, Level: 6}}, []uint64{100})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
	require.NoError(t, lg.commitBatch(seq))
	require.NoError(t, lg.bornAround(101, func() error { return nil }))
	require.NoError(t, lg.close())

	// Round-trip replay.
	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	require.NotZero(t, lg.jobID)
	require.Equal(t, 6, lg.targetLevel)
	require.Equal(t, uint64(100), lg.fence)
	require.False(t, lg.complete)
	require.Len(t, lg.batches, 1)
	require.True(t, lg.batches[0].committed)
	for _, id := range []uint64{10, 11, 100, 101} {
		_, ok := lg.knownTableIDs[id]
		require.True(t, ok, "id %d should be known", id)
	}
	require.NoError(t, lg.close())

	// Torn tail: garbage appended where the next frame should be must be
	// truncated, and appending afterwards resumes at the right offset.
	path := flattenLedgerPath(dir)
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = fp.Write([]byte{0x7f, 0x7f, 0x7f})
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	require.Len(t, lg.batches, 1)
	require.True(t, lg.batches[0].committed)
	_, err = lg.prepareBatch(4, 5, []ledgerTableRef{{ID: 12, Level: 4}}, []uint64{102})
	require.NoError(t, err)
	require.NoError(t, lg.close())
	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	require.Len(t, lg.batches, 2)
	require.NoError(t, lg.close())

	// Bad magic is a hard error (no silent truncation of the header).
	badDir := t.TempDir()
	badPath := filepath.Join(badDir, flattenLedgerFilename)
	require.NoError(t, os.WriteFile(badPath, []byte("XXXX0000junk"), 0600))
	bf, err := os.Open(badPath)
	require.NoError(t, err)
	blank := &flattenLedger{fp: bf, prepared: map[uint64]*flattenBatch{},
		knownTableIDs: map[uint64]struct{}{}}
	_, err = blank.replay(false)
	require.ErrorIs(t, err, errFlattenLedgerBadMagic)
	require.NoError(t, bf.Close())

	// CRC-corrupt last frame: the JobStart survives, the BatchPrepare is dropped.
	dir2 := t.TempDir()
	lg2, err := beginFlattenJob(dir2, 3, 50)
	require.NoError(t, err)
	goodEnd, err := lg2.fp.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	_, err = lg2.prepareBatch(1, 2, []ledgerTableRef{{ID: 7, Level: 1}}, []uint64{80})
	require.NoError(t, err)
	require.NoError(t, lg2.close())
	fp2, err := os.OpenFile(flattenLedgerPath(dir2), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = fp2.WriteAt([]byte{0xff}, goodEnd+8+1) // flip a payload byte
	require.NoError(t, err)
	require.NoError(t, fp2.Close())
	lg2, err = openFlattenLedger(dir2)
	require.NoError(t, err)
	require.Equal(t, uint64(50), lg2.fence)
	require.Empty(t, lg2.batches)
	require.NoError(t, lg2.close())
}

// ---- 2. reconcile against MANIFEST ----------------------------------------

func TestFlattenLedgerReconcile(t *testing.T) {
	dir := t.TempDir()
	lg, err := beginFlattenJob(dir, 5, 500)
	require.NoError(t, err)
	// Batch 1 will look committed in MANIFEST (outputs live, inputs gone).
	_, err = lg.prepareBatch(4, 5,
		[]ledgerTableRef{{ID: 10, Level: 4}, {ID: 11, Level: 5}}, []uint64{100})
	require.NoError(t, err)
	// Batch 2 will look not committed (inputs live, outputs absent).
	_, err = lg.prepareBatch(4, 5,
		[]ledgerTableRef{{ID: 20, Level: 4}, {ID: 21, Level: 5}}, []uint64{101})
	require.NoError(t, err)
	require.NoError(t, lg.close())

	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	live := map[uint64]TableManifest{
		100: tm(5), // output of batch 1, at destination
		20:  tm(4), // inputs of batch 2, still home
		21:  tm(5),
		501: tm(0), // flushed during the crash window, id >= fence
	}
	require.NoError(t, lg.reconcile(live))
	require.True(t, lg.batches[0].committed, "batch 1 must be repaired-committed")
	require.True(t, lg.batches[1].aborted, "batch 2 must be aborted")
	require.Empty(t, lg.prepared)
	_, knownBorn := lg.knownTableIDs[501]
	require.True(t, knownBorn, "crash-window flush table must be backfilled")
	require.NoError(t, lg.close())

	// Repair records are durable.
	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	require.True(t, lg.batches[0].committed)
	require.True(t, lg.batches[1].aborted)
	_, knownBorn = lg.knownTableIDs[501]
	require.True(t, knownBorn)
	require.NoError(t, lg.close())

	// Mixed state (an input AND its output both live) is a hard error.
	dir2 := t.TempDir()
	lg2, err := beginFlattenJob(dir2, 5, 500)
	require.NoError(t, err)
	_, err = lg2.prepareBatch(4, 5, []ledgerTableRef{{ID: 30, Level: 4}}, []uint64{102})
	require.NoError(t, err)
	require.NoError(t, lg2.close())
	lg2, err = openFlattenLedger(dir2)
	require.NoError(t, err)
	err = lg2.reconcile(map[uint64]TableManifest{30: tm(4), 102: tm(5)})
	require.ErrorIs(t, err, errFlattenLedgerInconsistent)
	require.NoError(t, lg2.close())

	// Output at the wrong level is a hard error.
	dir3 := t.TempDir()
	lg3, err := beginFlattenJob(dir3, 5, 500)
	require.NoError(t, err)
	_, err = lg3.prepareBatch(4, 5, []ledgerTableRef{{ID: 40, Level: 4}}, []uint64{103})
	require.NoError(t, err)
	require.NoError(t, lg3.close())
	lg3, err = openFlattenLedger(dir3)
	require.NoError(t, err)
	err = lg3.reconcile(map[uint64]TableManifest{103: tm(3)})
	require.ErrorIs(t, err, errFlattenLedgerInconsistent)
	require.NoError(t, lg3.close())
}

// ---- 3. end-to-end flatten with ledger ------------------------------------

func TestFlattenWithLedger(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(flattenTestOptions(dir))
	require.NoError(t, err)

	bands := [][]string{bandKeys('a', 8), bandKeys('m', 8), bandKeys('x', 8)}
	buildLevelTree(t, db, bands)
	require.Equal(t, []int{1, 2, 3}, nonEmptyLevels(db))

	st0, err := db.FlattenStatus()
	require.NoError(t, err)
	require.Nil(t, st0)

	require.NoError(t, db.Flatten(1))
	require.Equal(t, []int{3}, nonEmptyLevels(db))

	got := readAllKeys(t, db)
	require.Len(t, got, 24)
	for _, band := range bands {
		for _, k := range band {
			require.Equal(t, "val-"+k, got[k])
		}
	}

	// Ledger completed and archived.
	_, err = os.Stat(flattenLedgerPath(dir))
	require.True(t, os.IsNotExist(err))
	done := countDoneLedgers(t, dir)
	require.Len(t, done, 1)
	st1, err := db.FlattenStatus()
	require.NoError(t, err)
	require.Nil(t, st1)

	lg := replayLedgerFile(t, filepath.Join(dir, done[0]))
	require.True(t, lg.complete)
	require.Equal(t, 3, lg.targetLevel)
	require.NotEmpty(t, lg.batches)
	for _, b := range lg.batches {
		require.True(t, b.committed)
	}
	require.NoError(t, db.Close())
}

// ---- 4. crash / restart resume --------------------------------------------

func TestFlattenResumeAfterCrash(t *testing.T) {
	t.Run("committed batches resume; stale prepared batch aborts", func(t *testing.T) {
		dir := t.TempDir()
		opt := flattenTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)

		// Two disjoint tables at L1, one at L2, one at L3 -> several hops.
		createAndOpen(db, []keyValVersion{
			{key: "a-0000", val: "val-a-0000", version: 1},
			{key: "a-0001", val: "val-a-0001", version: 1},
			{key: "a-0002", val: "val-a-0002", version: 1},
			{key: "a-0003", val: "val-a-0003", version: 1},
			{key: "a-0004", val: "val-a-0004", version: 1},
			{key: "a-0005", val: "val-a-0005", version: 1},
		}, 1)
		createAndOpen(db, []keyValVersion{
			{key: "j-0000", val: "val-j-0000", version: 1},
			{key: "j-0001", val: "val-j-0001", version: 1},
			{key: "j-0002", val: "val-j-0002", version: 1},
			{key: "j-0003", val: "val-j-0003", version: 1},
			{key: "j-0004", val: "val-j-0004", version: 1},
			{key: "j-0005", val: "val-j-0005", version: 1},
		}, 1)
		mkv := make([]keyValVersion, 0, 6)
		for _, k := range bandKeys('m', 6) {
			mkv = append(mkv, keyValVersion{key: k, val: "val-" + k, version: 1})
		}
		xkv := make([]keyValVersion, 0, 6)
		for _, k := range bandKeys('x', 6) {
			xkv = append(xkv, keyValVersion{key: k, val: "val-" + k, version: 1})
		}
		createAndOpen(db, mkv, 2)
		createAndOpen(db, xkv, 3)
		refreshLevelSizes(t, db)
		require.Equal(t, []int{1, 2, 3}, nonEmptyLevels(db))

		getRecs, setHook := installLedgerHook(t)
		var prepareCount atomic.Int32
		setHook(func(rec flattenLedgerRecord) error {
			if rec.Type == flattenLedgerRecordBatchPrepare && prepareCount.Add(1) >= 3 {
				return errors.New("injected ledger disk failure")
			}
			return nil
		})

		require.Error(t, db.Flatten(1))
		// The injected failure is scoped to the first run; the resume run and the
		// stale-batch append below must run with the hook passive.
		setHook(nil)

		jobID := uint64(0)
		committed := map[uint64]bool{}
		for _, r := range getRecs() {
			switch r.Type {
			case flattenLedgerRecordJobStart:
				jobID = r.JobID
			case flattenLedgerRecordBatchCommit:
				committed[r.BatchSeq] = true
			}
		}
		require.NotZero(t, jobID)
		run1Inputs := map[uint64]bool{}
		for _, r := range getRecs() {
			if r.Type == flattenLedgerRecordBatchPrepare && committed[r.BatchSeq] {
				for _, in := range r.Inputs {
					run1Inputs[in.ID] = true
				}
			}
		}
		require.NotEmpty(t, run1Inputs)

		// Simulate a prepared-but-uncommitted batch left by a kill.
		snapshot := db.manifest.tableSnapshot()
		var staleRef ledgerTableRef
		for id, m := range snapshot {
			if int(m.Level) < 3 {
				staleRef = ledgerTableRef{ID: id, Level: int(m.Level)}
				break
			}
		}
		require.NotZero(t, staleRef.ID)
		lg, err := openFlattenLedger(dir)
		require.NoError(t, err)
		_, err = lg.prepareBatch(staleRef.Level, 3,
			[]ledgerTableRef{staleRef}, []uint64{900001})
		require.NoError(t, err)
		require.NoError(t, lg.close())

		require.NoError(t, db.Close())

		// Reopen: orphan SST cleanup runs; Flatten reconciles and resumes.
		db2, err := Open(opt)
		require.NoError(t, err)

		st, err := db2.FlattenStatus()
		require.NoError(t, err)
		require.NotNil(t, st)
		require.Equal(t, jobID, st.JobID)
		require.Greater(t, st.Pending, 0)

		require.NoError(t, db2.Flatten(1))
		require.Equal(t, []int{3}, nonEmptyLevels(db2))

		got := readAllKeys(t, db2)
		require.Len(t, got, 24)
		for _, p := range []byte{'a', 'j', 'm', 'x'} {
			for i := 0; i < 6; i++ {
				k := fmt.Sprintf("%c-%04d", p, i)
				require.Equal(t, "val-"+k, got[k])
			}
		}

		done := countDoneLedgers(t, db2.opt.Dir)
		require.Len(t, done, 1)
		final := replayLedgerFile(t, filepath.Join(db2.opt.Dir, done[0]))
		require.Equal(t, jobID, final.jobID)
		require.True(t, final.complete)

		// Run-1 consumed input ids must never be moved again after resume.
		allInputs := map[uint64]int{}
		hasAbort := false
		for _, b := range final.batches {
			if b.aborted {
				hasAbort = true
			}
			if b.committed {
				for _, in := range b.inputs {
					allInputs[in.ID]++
				}
			}
		}
		require.True(t, hasAbort, "stale batch must be recorded as aborted")
		for id := range run1Inputs {
			require.Equal(t, 1, allInputs[id], "table %d moved again after resume", id)
		}
		require.NoError(t, db2.Close())
	})

	t.Run("missing BatchCommit after MANIFEST commit is repaired", func(t *testing.T) {
		dir := t.TempDir()
		opt := flattenTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)
		buildLevelTree(t, db, [][]string{bandKeys('a', 6), bandKeys('m', 6), bandKeys('x', 6)})

		getRecs, setHook := installLedgerHook(t)
		var commits atomic.Int32
		setHook(func(rec flattenLedgerRecord) error {
			if rec.Type == flattenLedgerRecordBatchCommit && commits.Add(1) == 1 {
				return errors.New("injected ledger sync failure on commit")
			}
			return nil
		})
		require.Error(t, db.Flatten(1))
		// Failure injection belongs to the first run only; the resume run must be
		// able to append its repair BatchCommit.
		setHook(nil)

		var firstPrepare flattenLedgerRecord
		for _, r := range getRecs() {
			if r.Type == flattenLedgerRecordBatchPrepare {
				firstPrepare = r
				break
			}
		}
		require.NotZero(t, firstPrepare.BatchSeq)

		require.NoError(t, db.Close())
		db2, err := Open(opt)
		require.NoError(t, err)
		require.NoError(t, db2.Flatten(1))
		require.Equal(t, []int{3}, nonEmptyLevels(db2))
		require.Len(t, readAllKeys(t, db2), 18)

		done := countDoneLedgers(t, db2.opt.Dir)
		require.Len(t, done, 1)
		final := replayLedgerFile(t, filepath.Join(db2.opt.Dir, done[0]))
		b := final.batches[firstPrepare.BatchSeq-1]
		require.True(t, b.committed)
		require.Equal(t, firstPrepare.Inputs, b.inputs)
		require.Equal(t, firstPrepare.Outputs, b.outputs)
		require.NoError(t, db2.Close())
	})
}

// ---- 5. tables born while flatten is running ------------------------------

func TestFlattenTablesBornDuringRun(t *testing.T) {
	dir := t.TempDir()
	opt := flattenTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	buildLevelTree(t, db, [][]string{bandKeys('m', 6), bandKeys('x', 6)})
	require.Equal(t, []int{1, 2}, nonEmptyLevels(db))

	getRecs, setHook := installLedgerHook(t)
	jobStartReached := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	setHook(func(rec flattenLedgerRecord) error {
		if rec.Type == flattenLedgerRecordJobStart {
			// Block before Flatten installs its ledger pointer, so the flush
			// below lands through the ordinary path with an id >= fence.
			once.Do(func() { close(jobStartReached) })
			<-proceed
		}
		return nil
	})

	flattenDone := make(chan error, 1)
	go func() { flattenDone <- db.Flatten(1) }()
	<-jobStartReached

	// Managed write at ts=200; fixture tables sit at ts=1, so no version clash.
	txn := db.NewTransactionAt(200, true)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("born-%04d", i)
		require.NoError(t, txn.Set([]byte(k), []byte("val-"+k)))
	}
	require.NoError(t, txn.CommitAt(200, nil))
	rotateActiveMemtable(t, db)
	require.Equal(t, []int{0, 1, 2}, nonEmptyLevels(db))

	close(proceed)
	require.NoError(t, <-flattenDone)

	require.Equal(t, []int{2}, nonEmptyLevels(db))
	got := readAllKeys(t, db)
	require.Len(t, got, 20)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("born-%04d", i)
		require.Equal(t, "val-"+k, got[k])
	}

	fence := uint64(0)
	var bornInBatch uint64
	for _, r := range getRecs() {
		if r.Type == flattenLedgerRecordJobStart {
			fence = r.Fence
		}
		if r.Type == flattenLedgerRecordBatchPrepare {
			for _, in := range r.Inputs {
				if in.Level == 0 && in.ID >= fence {
					bornInBatch = in.ID
				}
			}
		}
	}
	require.NotZero(t, fence)
	require.NotZero(t, bornInBatch, "fence-born L0 table must participate in a batch")
	require.NoError(t, db.Close())
}

func TestFlattenBornAroundDirect(t *testing.T) {
	dir := t.TempDir()
	lg, err := beginFlattenJob(dir, 3, 50)
	require.NoError(t, err)

	calls := 0
	fn := func() error { calls++; return nil }
	require.NoError(t, lg.bornAround(60, fn))
	require.NoError(t, lg.bornAround(60, fn)) // same id -> no duplicate frame
	require.Equal(t, 2, calls)
	require.Contains(t, lg.knownTableIDs, uint64(60))
	require.NoError(t, lg.close())

	lg, err = openFlattenLedger(dir)
	require.NoError(t, err)
	require.Contains(t, lg.knownTableIDs, uint64(60))
	require.Len(t, lg.knownTableIDs, 1)
	require.NoError(t, lg.close())
}

// ---- 6. ledger append failure rolls the batch back ------------------------

func TestFlattenPrepareFailureRollback(t *testing.T) {
	dir := t.TempDir()
	opt := flattenTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	buildLevelTree(t, db, [][]string{bandKeys('a', 6), bandKeys('m', 6), bandKeys('x', 6)})

	before := db.manifest.tableSnapshot()

	_, setHook := installLedgerHook(t)
	setHook(func(rec flattenLedgerRecord) error {
		if rec.Type == flattenLedgerRecordBatchPrepare {
			return errors.New("injected ledger write failure")
		}
		return nil
	})
	require.Error(t, db.Flatten(1))
	flattenLedgerTestHook = nil

	// MANIFEST unchanged: same tables at the same levels.
	require.Equal(t, before, db.manifest.tableSnapshot())

	// No orphan SST files: every file id on disk is referenced by the MANIFEST.
	onDisk := getIDMap(db.opt.Dir)
	require.Len(t, onDisk, len(before))
	for id := range before {
		_, ok := onDisk[id]
		require.True(t, ok)
	}

	// Levels untouched and a retry succeeds.
	require.Equal(t, []int{1, 2, 3}, nonEmptyLevels(db))
	require.NoError(t, db.Flatten(1))
	require.Equal(t, []int{3}, nonEmptyLevels(db))
	require.Len(t, readAllKeys(t, db), 18)
	require.NoError(t, db.Close())
}

// ---- 7b. concurrent workers share one ledger ------------------------------

func TestFlattenMultipleWorkers(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(flattenTestOptions(dir))
	require.NoError(t, err)

	// Three disjoint tables at L1, two at L2, one at L3: three L1->L2
	// compactions can be reserved concurrently, exercising ledger appends from
	// multiple compactor goroutines.
	oneBand := func(prefix byte, n int) []keyValVersion {
		kvs := make([]keyValVersion, 0, n)
		for i := 0; i < n; i++ {
			kvs = append(kvs, keyValVersion{
				key: fmt.Sprintf("%c-%04d", prefix, i), val: "v" + string(prefix), version: 1,
			})
		}
		return kvs
	}
	createAndOpen(db, oneBand('a', 4), 1)
	createAndOpen(db, oneBand('b', 4), 1)
	createAndOpen(db, oneBand('c', 4), 1)
	createAndOpen(db, append(oneBand('m', 2), oneBand('n', 2)...), 2)
	createAndOpen(db, oneBand('x', 2), 3)
	refreshLevelSizes(t, db)
	require.Equal(t, []int{1, 2, 3}, nonEmptyLevels(db))

	require.NoError(t, db.Flatten(3))
	require.Equal(t, []int{3}, nonEmptyLevels(db))
	got := readAllKeys(t, db)
	// a/b/c 4 each + m,n 2 each + x 2 = 18.
	require.Len(t, got, 18)

	done := countDoneLedgers(t, db.opt.Dir)
	require.Len(t, done, 1)
	final := replayLedgerFile(t, filepath.Join(db.opt.Dir, done[0]))
	seqs := map[uint64]bool{}
	for _, b2 := range final.batches {
		require.True(t, b2.committed)
		require.False(t, seqs[b2.seq], "batch seq duplicated")
		seqs[b2.seq] = true
	}
	require.GreaterOrEqual(t, len(seqs), 3)
	require.NoError(t, db.Close())
}

// ---- 7. rerun starts a new job and keeps the old ledger archived ----------

func TestFlattenRerunStartsNewJob(t *testing.T) {
	dir := t.TempDir()
	opt := flattenTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	buildLevelTree(t, db, [][]string{bandKeys('a', 6), bandKeys('x', 6)})

	require.NoError(t, db.Flatten(1))
	first := countDoneLedgers(t, dir)
	require.Len(t, first, 1)
	firstID := replayLedgerFile(t, filepath.Join(dir, first[0])).jobID

	require.NoError(t, db.Flatten(1))
	second := countDoneLedgers(t, dir)
	require.Len(t, second, 2)
	ids := map[uint64]bool{}
	for _, name := range second {
		ids[replayLedgerFile(t, filepath.Join(dir, name)).jobID] = true
	}
	require.True(t, ids[firstID])
	require.Len(t, ids, 2, "rerun must start a distinct job")
	require.NoError(t, db.Close())
}
