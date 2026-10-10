/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func rotationKey() []byte {
	k := make([]byte, 32)
	rand.Read(k)
	return k
}

func openEncrypted(t *testing.T, dir, valueDir string, key []byte,
	rotDuration time.Duration) *DB {
	opts := DefaultOptions(dir)
	opts.EncryptionKey = key
	opts.BlockCacheSize = 1 << 20
	opts.IndexCacheSize = 1 << 20
	if valueDir != "" {
		opts.ValueDir = valueDir
	}
	if rotDuration > 0 {
		opts.EncryptionKeyRotationDuration = rotDuration
	}
	db, err := Open(opts)
	require.NoError(t, err)
	return db
}

func writeBig(t *testing.T, db *DB, key string, val []byte) {
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte(key), val)
	}))
}

func readBig(t *testing.T, db *DB, key string) []byte {
	var out []byte
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte(key))
		require.NoError(t, err)
		return item.Value(func(v []byte) error {
			out = append(out, v...)
			return nil
		})
	}))
	return out
}

func requireAbsent(t *testing.T, path string) {
	_, err := os.Stat(path)
	require.Truef(t, os.IsNotExist(err), "expected %s to be absent, got err %v", path, err)
}

func registryKeyCount(t *testing.T, dir string, key []byte) int {
	kr, err := probeRegistryAt(filepath.Join(dir, KeyRegistryFileName), key)
	require.NoError(t, err)
	return len(kr.dataKeys)
}

func appendGarbage(t *testing.T, path string, n int) {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = fp.Write(make([]byte, n))
	require.NoError(t, err)
	require.NoError(t, fp.Close())
}

func TestReconcileRotationBasic(t *testing.T) {
	dir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()

	db := openEncrypted(t, dir, "", key, 0)
	val := make([]byte, 200)
	rand.Read(val)
	writeBig(t, db, "k", val)
	require.NoError(t, db.Close())

	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rep.RotationID)
	require.True(t, rep.Completed)
	require.GreaterOrEqual(t, rep.DataKeysRewrapped, 1)
	require.Contains(t, rep.ReferencedDataKeys, uint64(1))

	// New key opens the DB and data is intact.
	db = openEncrypted(t, dir, "", key2, 0)
	require.Equal(t, val, readBig(t, db, "k"))
	require.NoError(t, db.Close())

	// Old key no longer opens it.
	opts := DefaultOptions(dir)
	opts.EncryptionKey = key
	opts.BlockCacheSize = 1 << 20
	_, err = Open(opts)
	require.EqualError(t, err, ErrEncryptionKeyMismatch.Error())

	// Rotation-only artifacts removed.
	requireAbsent(t, filepath.Join(dir, keyRegistryBackup))
	requireAbsent(t, filepath.Join(dir, rotationStageDirName))
	requireAbsent(t, filepath.Join(dir, KeyRegistryRewriteFileName))
}

// A hard crash mid-batch must leave a resumable state: already-wrapped keys are
// not wrapped again, journal torn tail is repaired, and the batch completes.
func TestReconcileResumeMidStage(t *testing.T) {
	dir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()

	db := openEncrypted(t, dir, "", key, time.Second)
	v1 := make([]byte, 200)
	rand.Read(v1)
	writeBig(t, db, "k1", v1)
	time.Sleep(1100 * time.Millisecond)
	v2 := make([]byte, 200)
	rand.Read(v2)
	writeBig(t, db, "k2", v2)
	require.NoError(t, db.Close())

	totalKeys := registryKeyCount(t, dir, key)
	require.GreaterOrEqual(t, totalKeys, 2)

	var staged atomic.Int32
	rotationHook = func(point rotationHookPoint, info *rotationHookInfo) error {
		if point == hookStagePlaced && staged.Add(1) == 1 {
			return SimulatedCrash
		}
		return nil
	}
	t.Cleanup(func() { rotationHook = nil })

	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.ErrorIs(t, err, SimulatedCrash)
	require.False(t, rep.Completed)

	// Stage holds one wrapped key; journal has no KEY_DONE for it yet.
	entries, err := os.ReadDir(filepath.Join(dir, rotationStageDirName))
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Simulate a torn journal write after the kill.
	appendGarbage(t, filepath.Join(dir, RotationFileName), 3)

	// Resume with the same key pair.
	rep, err = ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.True(t, rep.Resumed)
	require.True(t, rep.Completed)
	// Only the keys not already staged were wrapped in this pass.
	require.Equal(t, totalKeys-1, rep.DataKeysRewrapped)

	db = openEncrypted(t, dir, "", key2, 0)
	require.Equal(t, v1, readBig(t, db, "k1"))
	require.Equal(t, v2, readBig(t, db, "k2"))
	require.NoError(t, db.Close())
}

// Crashes after the registry swap (rename) and after the COMMIT record must both
// converge to a completed rotation on replay.
func TestReconcileCrashNearCommit(t *testing.T) {
	for _, point := range []rotationHookPoint{hookAfterSwap, hookAfterCommit} {
		t.Run(pointName(point), func(t *testing.T) {
			dir := t.TempDir()
			key, key2 := rotationKey(), rotationKey()

			db := openEncrypted(t, dir, "", key, 0)
			val := make([]byte, 200)
			rand.Read(val)
			writeBig(t, db, "k", val)
			require.NoError(t, db.Close())

			var fired atomic.Bool
			rotationHook = func(p rotationHookPoint, info *rotationHookInfo) error {
				if p == point && fired.CompareAndSwap(false, true) {
					return SimulatedCrash
				}
				return nil
			}
			t.Cleanup(func() { rotationHook = nil })

			_, err := ReconcileKeyRotation(RotationOptions{
				Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
			})
			require.ErrorIs(t, err, SimulatedCrash)

			rep, err := ReconcileKeyRotation(RotationOptions{
				Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
			})
			require.NoError(t, err)
			require.True(t, rep.Completed)
			require.Zero(t, rep.DataKeysRewrapped)

			db = openEncrypted(t, dir, "", key2, 0)
			require.Equal(t, val, readBig(t, db, "k"))
			require.NoError(t, db.Close())
		})
	}
}

func pointName(p rotationHookPoint) string {
	switch p {
	case hookAfterSwap:
		return "AfterSwap"
	case hookAfterCommit:
		return "AfterCommit"
	default:
		return "Other"
	}
}

// A failure while materializing the new registry (bad bytes written to REWRITE)
// rolls the whole batch back: old key keeps working, stage is cleared, ABORT is
// journaled, and a subsequent clean run succeeds.
func TestReconcileRollbackOnBadRewrite(t *testing.T) {
	dir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()

	db := openEncrypted(t, dir, "", key, 0)
	val := make([]byte, 200)
	rand.Read(val)
	writeBig(t, db, "k", val)
	require.NoError(t, db.Close())

	var injected atomic.Bool
	rotationHook = func(point rotationHookPoint, info *rotationHookInfo) error {
		if point == hookRewriteWritten && injected.CompareAndSwap(false, true) {
			// Simulate a disk-level corruption of the rewrite file, once only.
			_ = os.WriteFile(info.RewritePath, []byte("not a registry"), 0600)
			return errors.New("injected rewrite failure")
		}
		return nil
	}
	t.Cleanup(func() { rotationHook = nil })

	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.ErrorIs(t, err, ErrKeyRotationFailed)
	require.True(t, rep.RolledBack)

	// Old key still opens, data intact.
	db = openEncrypted(t, dir, "", key, 0)
	require.Equal(t, val, readBig(t, db, "k"))
	require.NoError(t, db.Close())

	// Stage cleared; backup retained so recovery stays possible.
	entries, err := os.ReadDir(filepath.Join(dir, rotationStageDirName))
	require.NoError(t, err)
	for _, e := range entries {
		_, ok := parseStageName(e.Name())
		require.False(t, ok, "stage file %s should have been removed", e.Name())
	}

	// Journal terminal state is ABORT, so a new clean batch can start.
	rep, err = ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.True(t, rep.Completed)
	require.Equal(t, uint64(2), rep.RotationID)

	db = openEncrypted(t, dir, "", key2, 0)
	require.Equal(t, val, readBig(t, db, "k"))
	require.NoError(t, db.Close())
}

// Files not owned by Badger (plain files, key material dropped next to the DB,
// foreign directories, in both dirs) must never be modified.
func TestReconcileExternalFilesUntouched(t *testing.T) {
	sstDir := t.TempDir()
	vdir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()

	db := openEncrypted(t, sstDir, vdir, key, 0)
	val := make([]byte, 200)
	rand.Read(val)
	writeBig(t, db, "k", val)
	require.NoError(t, db.Close())

	foreign := map[string][]byte{
		filepath.Join(sstDir, "notes.txt"):           []byte("operator notes"),
		filepath.Join(sstDir, "external.key"):        []byte("key material"),
		filepath.Join(sstDir, "foreign", "marker"):   []byte("marker"),
		filepath.Join(vdir, "external.bin"):          []byte("bytes in vlog dir"),
		filepath.Join(vdir, "foreign-dir", "marker"): []byte("bytes"),
	}
	before := make(map[string][32]byte)
	for path, content := range foreign {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, content, 0600))
		before[path] = sha256.Sum256(content)
	}

	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: sstDir, ValueDir: vdir,
		OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.True(t, rep.Completed)

	for path, want := range before {
		got, err := os.ReadFile(path)
		require.NoErrorf(t, err, "external file %s missing after rotation", path)
		require.Equalf(t, want, sha256.Sum256(got),
			"external file %s was modified", path)
	}

	db = openEncrypted(t, sstDir, vdir, key2, 0)
	require.Equal(t, val, readBig(t, db, "k"))
	require.NoError(t, db.Close())
}

// A vlog referencing a data key absent from the registry must stop the rotation
// before any durable change is made.
func TestReconcileMissingReferencedKey(t *testing.T) {
	dir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()

	db := openEncrypted(t, dir, "", key, 0)
	val := make([]byte, 200)
	rand.Read(val)
	writeBig(t, db, "k", val)
	require.NoError(t, db.Close())

	// Drop in a Badger-named vlog whose header references missing key id 99.
	var hdr [20]byte
	binary.BigEndian.PutUint64(hdr[0:8], 99)
	rand.Read(hdr[8:])
	crafted := filepath.Join(dir, "000999.vlog")
	require.NoError(t, os.WriteFile(crafted, hdr[:], 0600))

	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.ErrorIs(t, err, ErrReferencedDataKeyMissing)
	require.Nil(t, rep)

	// Nothing begun: no journal, no backup, registry still under old key.
	requireAbsent(t, filepath.Join(dir, RotationFileName))
	requireAbsent(t, filepath.Join(dir, keyRegistryBackup))
	require.Equal(t, 1, registryKeyCount(t, dir, key))

	// Once the unreadable file is removed, rotation proceeds.
	require.NoError(t, os.Remove(crafted))
	rep, err = ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.True(t, rep.Completed)
}

// Re-running with a different key pair while a batch is unfinished must be rejected
// rather than silently starting another batch.
func TestReconcilePendingKeyMismatch(t *testing.T) {
	dir := t.TempDir()
	key, key2 := rotationKey(), rotationKey()
	key3 := rotationKey()

	db := openEncrypted(t, dir, "", key, 0)
	val := make([]byte, 200)
	rand.Read(val)
	writeBig(t, db, "k", val)
	require.NoError(t, db.Close())

	rotationHook = func(point rotationHookPoint, info *rotationHookInfo) error {
		if point == hookStagePlaced {
			return SimulatedCrash
		}
		return nil
	}
	t.Cleanup(func() { rotationHook = nil })

	_, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.ErrorIs(t, err, SimulatedCrash)

	_, err = ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key3,
	})
	require.ErrorIs(t, err, ErrPendingRotationMismatch)

	// State untouched; the correct pair still resumes and completes.
	rep, err := ReconcileKeyRotation(RotationOptions{
		Dir: dir, OldEncryptionKey: key, NewEncryptionKey: key2,
	})
	require.NoError(t, err)
	require.True(t, rep.Completed)
}
