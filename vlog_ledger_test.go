/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func gcLedgerTestOpts(dir string) Options {
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20
	opt.BaseTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	return opt
}

func findLedgerEntry(t *testing.T, r *VlogGCLedgerReport, name string) VlogGCLedgerEntry {
	t.Helper()
	for _, e := range r.Entries {
		if e.Name == name {
			return e
		}
	}
	require.Failf(t, "ledger entry not found", "name=%s", name)
	return VlogGCLedgerEntry{}
}

func requireNoLedgerDrift(t *testing.T, r *VlogGCLedgerReport) {
	t.Helper()
	for _, e := range r.Entries {
		require.Falsef(t, e.Drift, "ledger drift for %s: state=%s onDisk=%v", e.Name, e.State, e.OnDisk)
	}
}

// writeVlogData writes n entries of random sz-sized values, committing every
// 20 keys. Values land in the value log under the test's ValueThreshold.
func writeVlogData(t *testing.T, db *DB, n, sz int) {
	t.Helper()
	txn := db.NewTransaction(true)
	for i := 0; i < n; i++ {
		v := make([]byte, sz)
		_, _ = rand.Read(v)
		require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%03d", i)), v)))
		if i%20 == 19 {
			require.NoError(t, txn.Commit())
			txn = db.NewTransaction(true)
		}
	}
	require.NoError(t, txn.Commit())
}

// collectSealedVlogs runs a ledger-tracked GC rewrite over every sealed
// (non-active) value-log file. It mirrors what RunValueLogGC does once
// pickLog has chosen a file, but without depending on discard statistics
// being produced by background compactions.
func collectSealedVlogs(t *testing.T, db *DB) {
	t.Helper()
	db.vlog.filesLock.RLock()
	fids := append([]uint32(nil), db.vlog.sortedFids()...)
	maxFid := db.vlog.maxFid
	db.vlog.filesLock.RUnlock()
	for _, fid := range fids {
		if fid == maxFid {
			continue
		}
		db.vlog.filesLock.RLock()
		lf := db.vlog.filesMap[fid]
		db.vlog.filesLock.RUnlock()
		if lf == nil {
			continue
		}
		require.NoError(t, db.vlog.doRunGC(lf))
	}
}

func countVlogFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".vlog" {
			n++
		}
	}
	return n
}

// ---- pure ledger tests -----------------------------------------------------

func TestVlogGCLedgerFoldAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)

	l, err := openVlogLedger(opt)
	require.NoError(t, err)
	require.NoError(t, l.markCreated(1))
	require.NoError(t, l.markCreated(2))
	require.NoError(t, l.gcBegin(1))
	require.NoError(t, l.gcFailed(1, errors.New("simulated write failure")))
	require.NoError(t, l.gcBegin(1))
	require.NoError(t, l.markDeleted(1, delByGC))
	require.NoError(t, l.markForeign("notes.vlog", 10, foreignBadName))
	require.NoError(t, l.ensureBootstrap())
	require.Equal(t, uint32(2), l.allTimeMaxFid())
	require.NoError(t, l.close())

	// Replay must fold the same bytes into exactly the same final state.
	l2, err := openVlogLedger(opt)
	require.NoError(t, err)
	defer l2.close()

	require.True(t, l2.isBootstrapped())
	require.Equal(t, uint32(2), l2.allTimeMaxFid())

	st, ok := l2.state(1)
	require.True(t, ok)
	require.Equal(t, fsDeleted, st)
	st, ok = l2.state(2)
	require.True(t, ok)
	require.Equal(t, fsAlive, st)
	require.True(t, l2.isForeign("notes.vlog"))
	require.False(t, l2.eligibleForGC(1))
	require.True(t, l2.eligibleForGC(2))

	snap := l2.snapshot()
	var f1 *ledgerFile
	for i := range snap.managed {
		if snap.managed[i].fid == 1 {
			f1 = &snap.managed[i]
		}
	}
	require.NotNil(t, f1)
	require.Equal(t, uint32(2), f1.attempts, "retried attempt must be recorded")
	require.Equal(t, gcSucceeded, f1.result)
}

func TestVlogGCLedgerTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)

	l, err := openVlogLedger(opt)
	require.NoError(t, err)
	require.NoError(t, l.markCreated(1))
	require.NoError(t, l.markDeleted(1, delByGC))
	require.NoError(t, l.close())

	path := filepath.Join(dir, ledgerFname)
	good, err := os.Stat(path)
	require.NoError(t, err)

	// Simulate a torn append: garbage at the tail.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte("garbage-torn-bytes"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	l2, err := openVlogLedger(opt)
	require.NoError(t, err)
	st, known := l2.state(1)
	require.True(t, known)
	require.Equal(t, fsDeleted, st)
	// Torn tail was dropped.
	require.Equal(t, good.Size(), l2.off)
	require.NoError(t, l2.markCreated(2))
	require.NoError(t, l2.close())

	l3, err := openVlogLedger(opt)
	require.NoError(t, err)
	defer l3.close()
	st, known = l3.state(2)
	require.True(t, known)
	require.Equal(t, fsAlive, st)
	_, known = l3.state(1)
	require.True(t, known)
}

func TestVlogGCLedgerCompactionConverges(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)

	l, err := openVlogLedger(opt)
	require.NoError(t, err)
	require.NoError(t, l.ensureBootstrap())
	require.NoError(t, l.markCreated(1))
	require.NoError(t, l.markCreated(2))
	require.NoError(t, l.markCreated(3))
	require.NoError(t, l.gcBegin(2))
	require.NoError(t, l.markDeleted(2, delByGC))
	require.NoError(t, l.markForeign("dropped.vlog", 42, foreignUnregistered))

	l.mu.Lock()
	require.NoError(t, l.compactLocked())
	l.mu.Unlock()
	require.NoError(t, l.close())

	l2, err := openVlogLedger(opt)
	require.NoError(t, err)
	defer l2.close()
	require.True(t, l2.isBootstrapped())
	require.Equal(t, uint32(3), l2.allTimeMaxFid())
	st, _ := l2.state(2)
	require.Equal(t, fsDeleted, st)
	st, _ = l2.state(3)
	require.Equal(t, fsAlive, st)
	require.True(t, l2.isForeign("dropped.vlog"))
}

// ---- end-to-end DB tests ---------------------------------------------------

func TestVlogGCLedgerForeignFilesUntouched(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10) // ~3.2 MiB across several vlog files
	require.NoError(t, db.Close())

	before := countVlogFiles(t, dir)
	require.GreaterOrEqual(t, before, 2)

	// Somebody else drops files into the directory: a numeric-named file with
	// a (deliberately unknown) encryption key id, and a non-numeric file.
	junk := make([]byte, 4096)
	for i := range junk {
		junk[i] = byte(i % 251)
	}
	junk[0] = 0xff // invalid/unknown key id in the vlog header
	junkPath := filepath.Join(dir, "000999.vlog")
	require.NoError(t, os.WriteFile(junkPath, junk, 0666))
	notesPath := filepath.Join(dir, "notes.vlog")
	require.NoError(t, os.WriteFile(notesPath, []byte("do not touch me"), 0666))

	db, err = Open(opt)
	require.NoError(t, err)

	db.vlog.filesLock.RLock()
	_, managed999 := db.vlog.filesMap[999]
	maxFid := db.vlog.maxFid
	db.vlog.filesLock.RUnlock()
	require.False(t, managed999, "foreign fid must not be managed")
	require.Less(t, maxFid, uint32(999), "foreign file must not inflate maxFid")

	// Foreign bytes are exactly as left, and stay so after another reopen.
	got, err := os.ReadFile(junkPath)
	require.NoError(t, err)
	require.Equal(t, junk, got)

	// Data is fully readable.
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key000"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Len(t, v, 32<<10)
		return nil
	}))

	// A log rotation allocates the next normal fid and does not collide with
	// the foreign 999 file.
	writeVlogData(t, db, 60, 32<<10)

	// GC works and leaves the foreign files alone.
	collectSealedVlogs(t, db)

	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	require.True(t, rep.Bootstrapped)
	e := findLedgerEntry(t, rep, "000999.vlog")
	require.False(t, e.Managed)
	require.Equal(t, "foreign", e.State)
	require.Equal(t, "invalid_vlog", e.ForeignReason)
	require.True(t, e.OnDisk)
	n := findLedgerEntry(t, rep, "notes.vlog")
	require.False(t, n.Managed)
	require.Equal(t, "bad_name", n.ForeignReason)
	requireNoLedgerDrift(t, rep)

	require.NoError(t, db.Close())
	got, err = os.ReadFile(junkPath)
	require.NoError(t, err)
	require.Equal(t, junk, got)
	notes, err := os.ReadFile(notesPath)
	require.NoError(t, err)
	require.Equal(t, "do not touch me", string(notes))

	// Second restart: decision is durable, no re-probe, no touch.
	db, err = Open(opt)
	require.NoError(t, err)
	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	require.False(t, findLedgerEntry(t, rep, "000999.vlog").Managed)
	requireNoLedgerDrift(t, rep)
	require.NoError(t, db.Close())
	got, err = os.ReadFile(junkPath)
	require.NoError(t, err)
	require.Equal(t, junk, got)
}

func TestVlogGCLedgerProcessedFilesNotRecycled(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)
	// Delete most keys so the early files become rewriteable.
	for i := 0; i < 90; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete([]byte(fmt.Sprintf("key%03d", i)))
		}))
	}
	collectSealedVlogs(t, db)

	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)

	var deleted []string
	for _, e := range rep.Entries {
		if e.Managed && e.State == "deleted" {
			deleted = append(deleted, e.Name)
			require.Equal(t, "succeeded", e.GCResult)
			require.False(t, e.OnDisk, "deleted file must be gone")
		}
	}
	require.NotEmpty(t, deleted, "at least one vlog file must have been collected")
	requireNoLedgerDrift(t, rep)

	require.NoError(t, db.Close())

	// Restart. Deleted files must not reappear in filesMap, on disk, or in a
	// subsequent GC report as pending work.
	db, err = Open(opt)
	require.NoError(t, err)

	for _, name := range deleted {
		_, statErr := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(statErr), "collected file came back: %s", name)
	}

	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	for _, name := range deleted {
		e := findLedgerEntry(t, rep, name)
		require.True(t, e.Managed)
		require.Equal(t, "deleted", e.State)
		require.False(t, e.OnDisk)
	}
	requireNoLedgerDrift(t, rep)

	// Everything is already collected: GC converges immediately and does not
	// reprocess a single file.
	err = db.RunValueLogGC(0.5)
	require.ErrorIs(t, err, ErrNoRewrite)

	// Surviving data still reads.
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key099"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Len(t, v, 32<<10)
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestVlogGCLedgerInterruptedGCResumesAndConverges(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)

	db.vlog.filesLock.RLock()
	fids := db.vlog.sortedFids()
	require.GreaterOrEqual(t, len(fids), 2)
	target := fids[0]
	db.vlog.filesLock.RUnlock()

	// Simulate a crash between the durable GC-start and the file removal.
	require.NoError(t, db.vlog.ledger.gcBegin(target))
	require.NoError(t, db.Close())

	// The file is still on disk; restart folds the interrupted attempt back to
	// alive instead of reprocessing blindly or deleting anything.
	db, err = Open(opt)
	require.NoError(t, err)

	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	e := findLedgerEntry(t, rep, fmt.Sprintf("%06d.vlog", target))
	require.Equal(t, "alive", e.State)
	require.Equal(t, uint32(1), e.GCAttempts, "attempt must remain durable after restart")
	require.True(t, e.OnDisk)
	requireNoLedgerDrift(t, rep)

	// Resume: the rewrite is re-runnable and converges to success.
	db.vlog.filesLock.RLock()
	lf := db.vlog.filesMap[target]
	db.vlog.filesLock.RUnlock()
	require.NotNil(t, lf)
	require.NoError(t, db.vlog.doRunGC(lf))

	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	e = findLedgerEntry(t, rep, fmt.Sprintf("%06d.vlog", target))
	require.Equal(t, "deleted", e.State)
	require.Equal(t, "succeeded", e.GCResult)
	require.Equal(t, uint32(2), e.GCAttempts)
	require.False(t, e.OnDisk)
	requireNoLedgerDrift(t, rep)

	require.NoError(t, db.Close())

	// Another restart stays converged: no zombie file, no reprocessing.
	db, err = Open(opt)
	require.NoError(t, err)
	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	e = findLedgerEntry(t, rep, fmt.Sprintf("%06d.vlog", target))
	require.Equal(t, "deleted", e.State)
	require.False(t, e.OnDisk)
	requireNoLedgerDrift(t, rep)
	require.NoError(t, db.Close())
}

func TestVlogGCLedgerFileReappearingAtDeletedFidIsForeign(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)
	for i := 0; i < 90; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete([]byte(fmt.Sprintf("key%03d", i)))
		}))
	}
	collectSealedVlogs(t, db)

	var deletedFid uint32
	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	for _, e := range rep.Entries {
		if e.Managed && e.State == "deleted" {
			deletedFid = e.Fid
			break
		}
	}
	require.NotZero(t, deletedFid)
	require.NoError(t, db.Close())

	// Somebody restores/plants a file at the already-collected fid.
	name := fmt.Sprintf("%06d.vlog", deletedFid)
	payload := []byte("not the original content at all")
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), payload, 0666))

	db, err = Open(opt)
	require.NoError(t, err)
	db.vlog.filesLock.RLock()
	_, managed := db.vlog.filesMap[deletedFid]
	db.vlog.filesLock.RUnlock()
	require.False(t, managed, "reappeared fid must not be managed")

	// Untouched.
	got, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.Equal(t, payload, got)

	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	e := findLedgerEntry(t, rep, name)
	require.False(t, e.Managed)
	require.Equal(t, "foreign", e.State)
	require.Equal(t, "reappeared", e.ForeignReason)
	require.True(t, e.OnDisk)
	requireNoLedgerDrift(t, rep)

	// GC never touches it.
	collectSealedVlogs(t, db)
	got, err = os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, db.Close())
}

func TestVlogGCLedgerAdoptsPreLedgerFilesAndReadsData(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)
	require.NoError(t, db.Close())

	// Simulate upgrading an installation that predates the ledger: remove it
	// while keeping all data files, then plant an invalid vlog.
	require.NoError(t, os.Remove(filepath.Join(dir, ledgerFname)))
	junk := make([]byte, 200)
	for i := 0; i < 8; i++ {
		junk[i] = 0xff // unknown data key id
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000888.vlog"), junk, 0666))

	db, err = Open(opt)
	require.NoError(t, err)

	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	require.True(t, rep.Bootstrapped, "first boot must durably take ownership")

	// Every valid pre-existing file was adopted and remains managed.
	managedAlive := 0
	for _, e := range rep.Entries {
		if e.Managed {
			require.Contains(t, []string{"legacy", "created"}, e.Origin)
			if e.OnDisk {
				managedAlive++
			}
		}
	}
	require.GreaterOrEqual(t, managedAlive, 2)

	// Junk was not adopted.
	j := findLedgerEntry(t, rep, "000888.vlog")
	require.False(t, j.Managed)
	require.Equal(t, "invalid_vlog", j.ForeignReason)
	requireNoLedgerDrift(t, rep)

	// Adopted data is readable and GC-able.
	require.NoError(t, db.View(func(txn *Txn) error {
		for _, i := range []int{0, 50, 99} {
			item, err := txn.Get([]byte(fmt.Sprintf("key%03d", i)))
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, 32<<10)
		}
		return nil
	}))
	for i := 0; i < 90; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete([]byte(fmt.Sprintf("key%03d", i)))
		}))
	}
	collectSealedVlogs(t, db)

	rep, err = db.VlogGCLedgerReport()
	require.NoError(t, err)
	requireNoLedgerDrift(t, rep)
	got, err := os.ReadFile(filepath.Join(dir, "000888.vlog"))
	require.NoError(t, err)
	require.Equal(t, junk, got, "foreign file must survive GC")
	require.NoError(t, db.Close())
}

func TestVlogGCLedgerConvergesWhenManagedFileVanishes(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)
	require.NoError(t, db.Close())

	// Operator removes a sealed (non-latest) managed file externally.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var victim string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".vlog" && e.Name() != fmt.Sprintf("%06d.vlog", highestVlogFid(t, dir)) {
			victim = e.Name()
			break
		}
	}
	require.NotEmpty(t, victim)
	require.NoError(t, os.Remove(filepath.Join(dir, victim)))

	db, err = Open(opt)
	require.NoError(t, err)
	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	e := findLedgerEntry(t, rep, victim)
	require.True(t, e.Managed)
	require.Equal(t, "deleted", e.State, "ledger must converge to disk truth")
	require.False(t, e.OnDisk)
	requireNoLedgerDrift(t, rep)
	require.NoError(t, db.Close())
}

func TestVlogGCLedgerDropAllRestartDoesNotReuseFids(t *testing.T) {
	dir := t.TempDir()
	opt := gcLedgerTestOpts(dir)

	db, err := Open(opt)
	require.NoError(t, err)
	writeVlogData(t, db, 100, 32<<10)
	collectSealedVlogs(t, db) // leaves deleted tombstones for low fids
	highWater := db.vlog.ledger.allTimeMaxFid()
	require.Greater(t, highWater, uint32(1))
	require.NoError(t, db.DropAll())
	require.NoError(t, db.Close())

	// Reopen used to inflate the runtime maxFid to a tombstoned, absent fid;
	// now maxFid tracks a present file while new allocations stay above the
	// ledger high-water mark.
	db, err = Open(opt)
	require.NoError(t, err)

	db.vlog.filesLock.RLock()
	_, ok := db.vlog.filesMap[db.vlog.maxFid]
	maxFid := db.vlog.maxFid
	db.vlog.filesLock.RUnlock()
	require.True(t, ok, "maxFid must always be a present file")
	require.Greater(t, maxFid, highWater, "fresh vlog must not reuse a deleted fid")

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("fresh"), make([]byte, 4096))
	}))
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("fresh"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Len(t, v, 4096)
		return nil
	}))

	rep, err := db.VlogGCLedgerReport()
	require.NoError(t, err)
	requireNoLedgerDrift(t, rep)
	require.NoError(t, db.Close())

	// And one more restart stays consistent.
	db, err = Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("fresh"))
		return err
	}))
	require.NoError(t, db.Close())
}

func highestVlogFid(t *testing.T, dir string) uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var max uint32
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".vlog" {
			continue
		}
		var fid uint32
		_, err := fmt.Sscanf(e.Name(), "%06d.vlog", &fid)
		require.NoError(t, err)
		if fid > max {
			max = fid
		}
	}
	return max
}
