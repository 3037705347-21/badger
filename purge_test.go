/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func openPurgeDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	return db
}

func seedPurgeKeys(t *testing.T, db *DB, prefix string, n int) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("%s%05d", prefix, i)
			if err := txn.Set([]byte(key), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
}

func putPurgeKey(t *testing.T, db *DB, key, val string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte(key), []byte(val))
	}))
}

func countPurgeKeys(t *testing.T, db *DB, prefix string) int {
	t.Helper()
	return len(listPurgeKeys(t, db, prefix))
}

func listPurgeKeys(t *testing.T, db *DB, prefix string) []string {
	t.Helper()
	var out []string
	err := db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.PrefetchValues = false
		if prefix != "" {
			opt.Prefix = []byte(prefix)
		}
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			out = append(out, string(it.Item().KeyCopy(nil)))
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(out)
	return out
}

func purgeOpts(prefixes ...string) PrefixPurgeOptions {
	out := PrefixPurgeOptions{BatchSize: 7}
	for _, p := range prefixes {
		out.Prefixes = append(out.Prefixes, []byte(p))
	}
	return out
}

// The happy path plus idempotent reruns: listed prefixes are emptied, keys
// outside the list survive, and a second run deletes nothing.
func TestPurgePrefixes_Basic(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)
	defer db.Close()

	seedPurgeKeys(t, db, "a/", 120)
	seedPurgeKeys(t, db, "b/", 40)
	seedPurgeKeys(t, db, "keep/", 30)

	rep, err := db.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	require.Equal(t, uint64(160), rep.DeletedKeys)
	require.ElementsMatch(t, []string{"a/", "b/"}, toStrings(rep.CompletedPrefixes))
	require.Empty(t, rep.RetiredPrefixes)
	require.FileExists(t, filepath.Join(dir, purgeLedgerFileName))

	require.Equal(t, 0, countPurgeKeys(t, db, "a/"))
	require.Equal(t, 0, countPurgeKeys(t, db, "b/"))
	require.Equal(t, 30, countPurgeKeys(t, db, "keep/"))

	// Rerun: everything is done, nothing is deleted again.
	rep2, err := db.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	require.Zero(t, rep2.DeletedKeys)
	require.ElementsMatch(t, []string{"a/", "b/"}, toStrings(rep2.SkippedPrefixes))
	require.Equal(t, 30, countPurgeKeys(t, db, "keep/"))
}

// A crash in the middle (at either side of the durability barrier), followed by
// process-level reopen and rerun, must converge to exactly the same end state as
// an uninterrupted run on a reference database.
func TestPurgePrefixes_CrashResumeAcrossRestart(t *testing.T) {
	const (
		nA = 100
		nB = 100
		nX = 25
	)

	setup := func(t *testing.T, dir string) *DB {
		db := openPurgeDB(t, dir)
		seedPurgeKeys(t, db, "a/", nA)
		seedPurgeKeys(t, db, "b/", nB)
		seedPurgeKeys(t, db, "x/", nX)
		return db
	}

	// Reference: uninterrupted purge.
	refDir := t.TempDir()
	refDB := setup(t, refDir)
	_, err := refDB.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	wantKeys := listPurgeKeys(t, refDB, "")
	require.Len(t, wantKeys, nX)
	require.NoError(t, refDB.Close())

	// Crash-prone run.
	dir := t.TempDir()
	db := setup(t, dir)

	barriers := 0
	crashHook := func(when string, prefix []byte, batch int) error {
		barriers++
		// Die on the 5th barrier before the ledger moves, and again on the
		// 12th barrier after the ledger moved.
		if barriers == 5 && when == purgeBarrierBeforeLedger {
			return fmt.Errorf("synthetic crash before ledger")
		}
		if barriers == 12 && when == purgeBarrierAfterLedger {
			return fmt.Errorf("synthetic crash after ledger")
		}
		return nil
	}
	opt := purgeOpts("a/", "b/")
	opt.hook = purgeHook{onBarrier: crashHook}
	rep, err := db.PurgePrefixes(opt)
	require.Error(t, err)
	require.NotNil(t, rep)
	require.NoError(t, db.Close())

	// The ledger must have recorded partial progress.
	data, err := os.ReadFile(filepath.Join(dir, purgeLedgerFileName))
	require.NoError(t, err)
	l, err := decodePurgeLedger(data)
	require.NoError(t, err)
	var sweeping int
	for _, e := range l.Prefixes {
		if e.State == purgeStateSweeping {
			sweeping++
			require.NotEmpty(t, e.Cursor)
		}
	}
	require.GreaterOrEqual(t, sweeping, 1)

	// Restart and finish, no hooks.
	db = openPurgeDB(t, dir)
	rep2, err := db.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	require.Equal(t, wantKeys, listPurgeKeys(t, db, ""))
	require.Equal(t, 0, countPurgeKeys(t, db, "a/"))
	require.Equal(t, 0, countPurgeKeys(t, db, "b/"))
	require.Equal(t, nX, countPurgeKeys(t, db, "x/"))

	// One more run: fully converged and stable.
	rep3, err := db.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	require.Zero(t, rep3.DeletedKeys)
	require.Equal(t, wantKeys, listPurgeKeys(t, db, ""))
	require.NoError(t, db.Close())

	// Deleted accounting is monotonic across restarts.
	require.Equal(t, uint64(nA+nB),
		rep.DeletedKeys+rep2.DeletedKeys+rep3.DeletedKeys)
}

// A prefix removed from the authoritative list mid-cleanup must never be
// touched again; re-adding it resumes and finishes the cleanup.
func TestPurgePrefixes_RemovedPrefixUntouched(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)
	seedPurgeKeys(t, db, "a/", 50)
	seedPurgeKeys(t, db, "b/", 60)
	seedPurgeKeys(t, db, "x/", 10)

	// Stop after ~3 batches, somewhere inside the key space.
	hits := 0
	opt := purgeOpts("a/", "b/")
	opt.hook = purgeHook{onBarrier: func(when string, _ []byte, _ int) error {
		if when == purgeBarrierAfterLedger {
			hits++
			if hits == 3 {
				return fmt.Errorf("synthetic crash")
			}
		}
		return nil
	}}
	_, err := db.PurgePrefixes(opt)
	require.Error(t, err)
	remainingB := countPurgeKeys(t, db, "b/")
	remainingX := countPurgeKeys(t, db, "x/")
	require.NoError(t, db.Close())

	// Restart with a list that no longer contains b/.
	db = openPurgeDB(t, dir)
	rep, err := db.PurgePrefixes(purgeOpts("a/"))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b/"}, toStrings(rep.RetiredPrefixes))
	require.Equal(t, 0, countPurgeKeys(t, db, "a/"))
	// b/ and x/ are exactly as they were at the crash point: nothing touched.
	require.Equal(t, remainingB, countPurgeKeys(t, db, "b/"))
	require.Equal(t, remainingX, countPurgeKeys(t, db, "x/"))

	// Re-add b/: the previously half-swept prefix resumes and completes.
	rep2, err := db.PurgePrefixes(purgeOpts("a/", "b/"))
	require.NoError(t, err)
	require.Equal(t, 0, countPurgeKeys(t, db, "b/"))
	require.Equal(t, remainingX, countPurgeKeys(t, db, "x/"))
	require.Equal(t, uint64(remainingB), rep2.DeletedKeys)
	require.NoError(t, db.Close())
}

// Data written after the fence timestamp must survive — including new keys
// written under a prefix that is actively being purged — and stay alive on
// reruns. Old multi-version history is removed with the key.
func TestPurgePrefixes_NewDataAfterFencePreserved(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)

	// Old data, including keys with multiple historical versions.
	seedPurgeKeys(t, db, "p/", 100)
	for i := 0; i < 50; i++ {
		putPurgeKey(t, db, fmt.Sprintf("p/%05d", i), "v2")
	}
	seedPurgeKeys(t, db, "x/", 10)

	injected := false
	opt := purgeOpts("p/")
	opt.hook = purgeHook{onBarrier: func(when string, _ []byte, _ int) error {
		if when == purgeBarrierBeforeLedger && !injected {
			injected = true
			// Concurrent-looking new writes landing inside and outside the
			// listed prefix, at various points in key order.
			putPurgeKey(t, db, "p/00000-new", "fresh") // sorts before cursor
			putPurgeKey(t, db, "p/zzzzz", "fresh")     // sorts after cursor
			putPurgeKey(t, db, "x/post-fence", "fresh")
		}
		return nil
	}}
	rep, err := db.PurgePrefixes(opt)
	require.NoError(t, err)
	require.True(t, injected)
	require.Equal(t, uint64(100), rep.DeletedKeys)

	got := listPurgeKeys(t, db, "")
	require.ElementsMatch(t, []string{
		"p/00000-new", "p/zzzzz",
		"x/00000", "x/00001", "x/00002", "x/00003", "x/00004",
		"x/00005", "x/00006", "x/00007", "x/00008", "x/00009",
		"x/post-fence",
	}, got)

	// Reruns leave the post-fence data alone.
	rep2, err := db.PurgePrefixes(purgeOpts("p/"))
	require.NoError(t, err)
	require.Zero(t, rep2.DeletedKeys)
	require.Len(t, listPurgeKeys(t, db, "p/"), 2)
}

// If the ledger cannot be created, the whole run fails before any key is
// deleted.
func TestPurgePrefixes_LedgerUnwritableFailsRun(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)
	defer db.Close()
	seedPurgeKeys(t, db, "a/", 20)

	// A regular file where the ledger directory is expected.
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0600))

	opt := purgeOpts("a/")
	opt.LedgerDir = blocker
	_, err := db.PurgePrefixes(opt)
	require.Error(t, err)
	require.Equal(t, 20, countPurgeKeys(t, db, "a/"), "no deletes may happen without a ledger")
	require.NoFileExists(t, filepath.Join(blocker, purgeLedgerFileName))

	// Same list against the default location works fine.
	_, err = db.PurgePrefixes(purgeOpts("a/"))
	require.NoError(t, err)
	require.Equal(t, 0, countPurgeKeys(t, db, "a/"))
}

// A corrupt or truncated ledger is a hard failure: never guess progress.
func TestPurgePrefixes_CorruptLedgerFails(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)
	seedPurgeKeys(t, db, "a/", 20)
	seedPurgeKeys(t, db, "x/", 5)
	_, err := db.PurgePrefixes(purgeOpts("a/"))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	ledgerPath := filepath.Join(dir, purgeLedgerFileName)
	good, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)

	corrupt := append([]byte(nil), good...)
	corrupt[len(corrupt)-1] ^= 0xff // break the CRC trailer
	require.NoError(t, os.WriteFile(ledgerPath, corrupt, 0600))

	db = openPurgeDB(t, dir)
	_, err = db.PurgePrefixes(purgeOpts("a/"))
	require.Error(t, err)
	require.Equal(t, 5, countPurgeKeys(t, db, "x/"), "no work happens with an unreadable ledger")
	require.NoError(t, db.Close())

	// Truncation is rejected too.
	require.NoError(t, os.WriteFile(ledgerPath, good[:purgeLedgerHeaderSize], 0600))
	db = openPurgeDB(t, dir)
	_, err = db.PurgePrefixes(purgeOpts("a/"))
	require.Error(t, err)
	require.NoError(t, db.Close())
}

func TestPurgePrefixes_InvalidOptions(t *testing.T) {
	dir := t.TempDir()

	t.Run("in-memory db", func(t *testing.T) {
		opt := getTestOptions("")
		opt.InMemory = true
		db, err := Open(opt)
		require.NoError(t, err)
		defer db.Close()
		_, err = db.PurgePrefixes(purgeOpts("a/"))
		require.Error(t, err)
	})

	t.Run("read-only db", func(t *testing.T) {
		db := openPurgeDB(t, dir)
		seedPurgeKeys(t, db, "a/", 5)
		require.NoError(t, db.Close())

		opt := getTestOptions(dir)
		opt.ReadOnly = true
		roDB, err := Open(opt)
		require.NoError(t, err)
		defer roDB.Close()
		_, err = roDB.PurgePrefixes(purgeOpts("a/"))
		require.Error(t, err)
		require.Equal(t, 5, countPurgeKeys(t, roDB, "a/"))
	})

	t.Run("bad prefixes on a writable db", func(t *testing.T) {
		d := t.TempDir()
		db := openPurgeDB(t, d)
		defer db.Close()

		_, err := db.PurgePrefixes(purgeOpts(""))
		require.Error(t, err)

		dup := purgeOpts("a/", "a/")
		_, err = db.PurgePrefixes(dup)
		require.Error(t, err)

		reserved := PrefixPurgeOptions{
			Prefixes: [][]byte{[]byte(string(badgerPrefix) + "x")},
		}
		_, err = db.PurgePrefixes(reserved)
		require.Error(t, err)
	})
}

// Empty prefix list is an explicit no-op and never creates a ledger.
func TestPurgePrefixes_EmptyListNoop(t *testing.T) {
	dir := t.TempDir()
	db := openPurgeDB(t, dir)
	defer db.Close()
	seedPurgeKeys(t, db, "a/", 3)

	rep, err := db.PurgePrefixes(PrefixPurgeOptions{BatchSize: 7})
	require.NoError(t, err)
	require.NotNil(t, rep)
	require.Zero(t, rep.DeletedKeys)
	require.Equal(t, 3, countPurgeKeys(t, db, "a/"))
	require.NoFileExists(t, filepath.Join(dir, purgeLedgerFileName))
}

func toStrings(in [][]byte) []string {
	out := make([]string, 0, len(in))
	for _, b := range in {
		out = append(out, string(b))
	}
	return out
}
