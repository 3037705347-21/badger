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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errInjectedCrash = errors.New("injected crash")

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func exportDBMap(t *testing.T, db *DB, prefix []byte) map[string]ExportEntry {
	t.Helper()
	want := make(map[string]ExportEntry)
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			want[string(item.Key())] = ExportEntry{
				Key:       item.KeyCopy(nil),
				Value:     val,
				UserMeta:  item.UserMeta(),
				Version:   item.Version(),
				ExpiresAt: item.ExpiresAt(),
			}
		}
		return nil
	})
	require.NoError(t, err)
	return want
}

func exportFileMap(t *testing.T, dir string, prefix []byte) map[string]ExportEntry {
	t.Helper()
	got := make(map[string]ExportEntry)
	err := ReadExport(dir, func(e ExportEntry) error {
		if prefix != nil && !bytes.HasPrefix(e.Key, prefix) {
			return fmt.Errorf("unexpected key %x outside prefix %x", e.Key, prefix)
		}
		got[string(e.Key)] = ExportEntry{
			Key:       append([]byte(nil), e.Key...),
			Value:     append([]byte(nil), e.Value...),
			UserMeta:  e.UserMeta,
			Version:   e.Version,
			ExpiresAt: e.ExpiresAt,
		}
		return nil
	})
	require.NoError(t, err)
	return got
}

func requireExportMatches(t *testing.T, dir string, db *DB, prefix []byte) {
	t.Helper()
	require.Equal(t, exportDBMap(t, db, prefix), exportFileMap(t, dir, prefix))
}

func requirePathsExist(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		_, err := os.Stat(p)
		require.NoErrorf(t, err, "expected %q to exist", p)
	}
}

func requirePathsAbsent(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		_, err := os.Stat(p)
		require.Truef(t, os.IsNotExist(err), "expected %q to be absent, got err=%v", p, err)
	}
}

func keyValue(i int, size int) []byte {
	v := append([]byte(fmt.Sprintf("val%03d-", i)), bytes.Repeat([]byte{byte('a' + i%26)}, size)...)
	return v
}

func setKeys(t *testing.T, db *DB, from, to int, big bool) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := from; i < to; i++ {
			size := 32
			if big && i%7 == 0 {
				size = 4096 // crosses the value-log threshold
			}
			e := NewEntry([]byte(fmt.Sprintf("key%03d", i)), keyValue(i, size)).
				WithMeta(byte(i % 256))
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	}))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestExportFullIncremental(t *testing.T) {
	tmp := t.TempDir()
	dbDir := filepath.Join(tmp, "db")
	exportDir := filepath.Join(tmp, "export")

	db, err := Open(getTestOptions(dbDir))
	require.NoError(t, err)

	const N = 40
	setKeys(t, db, 0, N, true)

	// External content placed in the export directory must survive, including
	// non-managed files nested inside the keys subtree.
	externalRoot := filepath.Join(exportDir, "notes.txt")
	externalNested := filepath.Join(exportDir, "external", "sub.txt")
	externalInKeys := filepath.Join(exportDir, exportKeysDir, "notes.txt")
	externalInShard := filepath.Join(exportDir, exportKeysDir, "ab", "keep.txt")
	// A file that mimics managed naming but is not part of any checkpoint is
	// export garbage and must be converged away.
	managedOrphan := filepath.Join(exportDir, exportKeysDir, "cd", "cd")
	for _, p := range []string{externalRoot, externalNested, externalInKeys,
		externalInShard, managedOrphan} {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, []byte("external"), 0644))
	}

	// Full export.
	res, err := db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.True(t, res.Full)
	require.Equal(t, uint64(0), res.SinceTs)
	require.Equal(t, N, res.Upserts)
	require.Equal(t, 0, res.Deletions)
	require.Equal(t, N, res.Keys)
	require.NotZero(t, res.NewSinceTs)
	requireExportMatches(t, exportDir, db, nil)

	requirePathsExist(t, externalRoot, externalNested, externalInKeys, externalInShard,
		filepath.Join(exportDir, exportCheckpointFile))
	requirePathsAbsent(t, managedOrphan)
	requirePathsAbsent(t, filepath.Join(exportDir, exportStagingDir))

	// Add 10, update 10, delete 5.
	setKeys(t, db, N, N+10, false)
	setKeys(t, db, 0, 10, false)
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 20; i < 25; i++ {
			if err := txn.Delete([]byte(fmt.Sprintf("key%03d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.NotZero(t, res.SinceTs)
	require.Equal(t, 20, res.Upserts)
	require.Equal(t, 5, res.Deletions)
	require.Equal(t, N+10-5, res.Keys)
	requireExportMatches(t, exportDir, db, nil)
	requirePathsExist(t, externalRoot, externalNested, externalInKeys, externalInShard)
	for i := 20; i < 25; i++ {
		requirePathsAbsent(t, filepath.Join(exportDir, keyRelPath([]byte(fmt.Sprintf("key%03d", i)))))
	}

	// Expired keys must converge out as well.
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 25; i < 28; i++ {
			e := NewEntry([]byte(fmt.Sprintf("key%03d", i)), []byte("x")).WithTTL(-time.Hour)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	}))
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.Equal(t, 0, res.Upserts)
	require.Equal(t, 3, res.Deletions)
	requireExportMatches(t, exportDir, db, nil)

	// No-op incremental run.
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, 0, res.Upserts)
	require.Equal(t, 0, res.Deletions)
	require.Equal(t, res.SinceTs, res.NewSinceTs)
	requireExportMatches(t, exportDir, db, nil)

	// Checkpoint survives process restarts.
	require.NoError(t, db.Close())
	db, err = Open(getTestOptions(dbDir))
	require.NoError(t, err)
	defer db.Close()

	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, 0, res.Upserts)
	require.Equal(t, 0, res.Deletions)
	requireExportMatches(t, exportDir, db, nil)
	requirePathsExist(t, externalRoot, externalNested, externalInKeys, externalInShard)
	requirePathsAbsent(t, filepath.Join(exportDir, exportStagingDir))
}

func TestExportInterruptedRunsConverge(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	exportDir := filepath.Join(tmp, "export")
	const N = 30
	setKeys(t, db, 0, N, true)

	res, err := db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.True(t, res.Full)
	before := exportFileMap(t, exportDir, nil)
	cpBytes, err := os.ReadFile(filepath.Join(exportDir, exportCheckpointFile))
	require.NoError(t, err)

	// Crash while staging: nothing committed yet.
	setKeys(t, db, N, N+5, false)
	setKeys(t, db, 0, 5, false)
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 10; i < 13; i++ {
			require.NoError(t, txn.Delete([]byte(fmt.Sprintf("key%03d", i))))
		}
		return nil
	}))
	e := newExporter(db, ExportOptions{Dir: exportDir})
	e.afterStaging = func(int) error { return errInjectedCrash }
	_, err = e.run()
	require.ErrorIs(t, err, errInjectedCrash)

	require.Equal(t, before, exportFileMap(t, exportDir, nil), "committed export must not change")
	cpAfter, err := os.ReadFile(filepath.Join(exportDir, exportCheckpointFile))
	require.NoError(t, err)
	require.Equal(t, cpBytes, cpAfter, "checkpoint must not advance on failure")
	requirePathsAbsent(t, filepath.Join(exportDir, exportStagingDir))

	// Rerun converges.
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, 10, res.Upserts)
	require.Equal(t, 3, res.Deletions)
	requireExportMatches(t, exportDir, db, nil)

	// Crash after file publication but before the checkpoint commit.
	setKeys(t, db, N+5, N+9, false)
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 13; i < 15; i++ {
			require.NoError(t, txn.Delete([]byte(fmt.Sprintf("key%03d", i))))
		}
		return nil
	}))
	e = newExporter(db, ExportOptions{Dir: exportDir})
	e.afterApply = func() error { return errInjectedCrash }
	_, err = e.run()
	require.ErrorIs(t, err, errInjectedCrash)
	requirePathsAbsent(t, filepath.Join(exportDir, exportStagingDir))

	// Rerun after a half-published commit still converges exactly.
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	requireExportMatches(t, exportDir, db, nil)
	requirePathsAbsent(t, filepath.Join(exportDir, exportStagingDir))
}

func TestExportRebuildsWhenFilesMissing(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	exportDir := filepath.Join(tmp, "export")
	setKeys(t, db, 0, 20, false)
	_, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)

	// Externally remove two owned files ("a missing segment"), then add a key.
	require.NoError(t, os.Remove(filepath.Join(exportDir, keyRelPath([]byte("key003")))))
	require.NoError(t, os.Remove(filepath.Join(exportDir, keyRelPath([]byte("key007")))))
	setKeys(t, db, 20, 23, false)

	res, err := db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.True(t, res.Full, "missing owned files must trigger a full rebuild")
	require.Equal(t, 23, res.Keys)
	requireExportMatches(t, exportDir, db, nil)

	// Following runs are incremental again.
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, 0, res.Upserts)
}

func TestExportCorruptCheckpoint(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	exportDir := filepath.Join(tmp, "export")
	setKeys(t, db, 0, 5, false)
	_, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)

	external := filepath.Join(exportDir, "mine.txt")
	require.NoError(t, os.WriteFile(external, []byte("keep"), 0644))

	cp := filepath.Join(exportDir, exportCheckpointFile)
	require.NoError(t, os.WriteFile(cp, []byte("garbage"), 0644))

	_, err = db.Export(ExportOptions{Dir: exportDir})
	require.Error(t, err)
	requirePathsExist(t, external, filepath.Join(exportDir, exportKeysDir))
}

func TestExportPrefix(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("user:1"), []byte("u1"))
	}))
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("admin:1"), []byte("a1"))
	}))

	userDir := filepath.Join(tmp, "user")
	res, err := db.Export(ExportOptions{Dir: userDir, Prefix: []byte("user:")})
	require.NoError(t, err)
	require.Equal(t, 1, res.Keys)
	requireExportMatches(t, userDir, db, []byte("user:"))

	require.NoError(t, db.Update(func(txn *Txn) error {
		if err := txn.Set([]byte("user:2"), []byte("u2")); err != nil {
			return err
		}
		return txn.Set([]byte("admin:2"), []byte("a2"))
	}))
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Delete([]byte("user:1"))
	}))

	res, err = db.Export(ExportOptions{Dir: userDir, Prefix: []byte("user:")})
	require.NoError(t, err)
	require.Equal(t, 1, res.Upserts)
	require.Equal(t, 1, res.Deletions)
	require.Equal(t, 1, res.Keys)
	requireExportMatches(t, userDir, db, []byte("user:"))

	// Reusing a checkpointed directory with a different prefix is rejected.
	_, err = db.Export(ExportOptions{Dir: userDir, Prefix: []byte("admin:")})
	require.Error(t, err)
}

func TestExportEmptyDB(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	exportDir := filepath.Join(tmp, "export")
	res, err := db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.True(t, res.Full)
	require.Equal(t, 0, res.Keys)
	requirePathsExist(t, filepath.Join(exportDir, exportCheckpointFile))
	require.Equal(t, 0, len(exportFileMap(t, exportDir, nil)))

	// First change after an empty full export flows through incrementally.
	setKeys(t, db, 0, 3, false)
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, 3, res.Upserts)
	require.Equal(t, 3, res.Keys)
	requireExportMatches(t, exportDir, db, nil)
}

func TestExportDeletingLastKeyInShard(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(getTestOptions(filepath.Join(tmp, "db")))
	require.NoError(t, err)
	defer db.Close()

	exportDir := filepath.Join(tmp, "export")
	// "x" is the only key in shard 78.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("x"), []byte("v"))
	}))
	res, err := db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.Equal(t, 1, res.Keys)
	shard := filepath.Join(exportDir, exportKeysDir, "78")
	requirePathsExist(t, shard)

	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Delete([]byte("x")) }))
	res, err = db.Export(ExportOptions{Dir: exportDir})
	require.NoError(t, err)
	require.Equal(t, 1, res.Deletions)
	require.Equal(t, 0, res.Keys)
	requireExportMatches(t, exportDir, db, nil)
	// The emptied shard directory is converged away as well.
	requirePathsAbsent(t, shard)
}

func TestExportRequiresDir(t *testing.T) {
	db, err := Open(getTestOptions(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Export(ExportOptions{})
	require.Error(t, err)
}
