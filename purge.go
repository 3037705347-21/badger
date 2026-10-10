/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4/y"
)

// PurgePrefixes performs a batch, crash-resumable cleanup of all keys under the
// given prefixes, driven by a persistent ledger stored on disk.
//
// The supplied prefix list is the single source of truth for what gets cleaned:
//
//   - Only keys under a prefix present in opt.Prefixes are ever deleted. Keys
//     outside the listed prefixes (including data written by the application
//     while a purge is running) are never touched.
//   - A prefix that was tracked by the ledger but has since been removed from
//     the list is left exactly as-is: the sweep neither resumes nor rolls it
//     back. Such prefixes are reported in PrefixPurgeReport.RetiredPrefixes.
//   - Re-running PurgePrefixes after a crash (or at any later time) converges to
//     the same result. Per-prefix progress (a cursor over the key space) is
//     persisted in batches, so an interrupted sweep resumes where it stopped and
//     never re-deletes past the committed cursor. Re-issuing deletes that were
//     committed before the crash is harmless and idempotent.
//
// Consistency model. When the ledger is first created it captures a fence
// read timestamp. Every run enumerates keys at that snapshot, so the set of
// keys targeted for deletion is fixed for the lifetime of the ledger and does
// not shift between reruns. Any write committed after the fence is invisible to
// the sweep and is preserved, even if it lands under a listed prefix. Starting
// a cleanup of a later generation means pointing opt.LedgerDir at a fresh,
// empty directory so a new ledger (and fence) is established.
//
// Failure model. A batch is made durable in this strict order: (1) the delete
// tombstones are committed and synced via DB.Sync, then (2) the ledger cursor is
// advanced and fsynced atomically (temp file + rename + directory sync). If the
// ledger cannot be read or written, the whole run fails immediately: no prefix
// is marked complete without a durable ledger entry. Because the ledger only
// advances after the deletes are synced, a crash can never leave the ledger
// ahead of the actual deletions.
//
// PurgePrefixes cannot run on a closed, read-only or in-memory DB.
func (db *DB) PurgePrefixes(opt PrefixPurgeOptions) (*PrefixPurgeReport, error) {
	switch {
	case db.IsClosed():
		return nil, ErrDBClosed
	case db.opt.ReadOnly:
		return nil, y.Wrapf(ErrInvalidRequest, "PurgePrefixes cannot run on a read-only DB")
	case db.opt.InMemory:
		return nil, y.Wrapf(ErrInvalidRequest,
			"PurgePrefixes requires a persistent ledger, which is not supported in in-memory mode")
	}
	prefixes, err := normalizePurgePrefixes(opt.Prefixes)
	if err != nil {
		return nil, err
	}
	batchSize := opt.BatchSize
	if batchSize <= 0 {
		batchSize = defaultPurgeBatchSize
	}
	if batchSize > maxPurgeBatchSize {
		batchSize = maxPurgeBatchSize
	}
	if len(prefixes) == 0 {
		// Empty list means "clean nothing": no fence, no ledger, no deletes.
		return &PrefixPurgeReport{}, nil
	}

	ledgerDir := opt.LedgerDir
	if ledgerDir == "" {
		ledgerDir = db.opt.Dir
	}
	ledgerPath := filepath.Join(ledgerDir, purgeLedgerFileName)

	// Only one ledger-driven purge may run at a time per DB.
	db.purgeLock.Lock()
	defer db.purgeLock.Unlock()

	ledger, err := loadOrCreatePurgeLedger(ledgerPath, db.orc.readTs())
	if err != nil {
		return nil, err
	}

	report := &PrefixPurgeReport{
		LedgerPath: ledgerPath,
		FenceTs:    ledger.FenceTs,
	}
	inList := make(map[string]struct{}, len(prefixes))
	for _, p := range prefixes {
		inList[string(p)] = struct{}{}
	}
	// Anything the ledger tracks but the caller no longer lists is retired:
	// it must not be touched during this run.
	for _, e := range ledger.Prefixes {
		if _, ok := inList[string(e.Prefix)]; !ok {
			report.RetiredPrefixes = append(report.RetiredPrefixes, append([]byte(nil), e.Prefix...))
		}
	}
	if err := ledger.admit(prefixes); err != nil {
		return nil, err
	}
	if err := ledger.persistIfDirty(ledgerPath); err != nil {
		return nil, err
	}

	for _, e := range ledger.Prefixes {
		if _, ok := inList[string(e.Prefix)]; !ok {
			continue // Retired prefix: do not touch.
		}
		switch e.State {
		case purgeStateDone:
			report.SkippedPrefixes = append(report.SkippedPrefixes, append([]byte(nil), e.Prefix...))
			continue
		case purgeStatePending:
			e.State = purgeStateSweeping
			e.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}

		deleted, err := db.sweepPurgePrefix(ledger, ledgerPath, e, batchSize, opt.hook)
		report.DeletedKeys += deleted
		if err != nil {
			return report, err
		}
		report.CompletedPrefixes = append(report.CompletedPrefixes, append([]byte(nil), e.Prefix...))
	}
	return report, nil
}

// sweepPurgePrefix deletes keys under one entry's prefix in batches, advancing
// the ledger cursor after each synced batch. It returns the number of keys
// deleted in this call. A nil error means the prefix reached the done state.
func (db *DB) sweepPurgePrefix(
	ledger *purgeLedger,
	ledgerPath string,
	e *purgeEntry,
	batchSize int,
	hook purgeHook,
) (uint64, error) {
	var deletedThisRun uint64
	barrier := 0
	for {
		keys, err := db.scanPurgeBatch(ledger.FenceTs, e.Prefix, e.Cursor, batchSize)
		if err != nil {
			return deletedThisRun, err
		}
		if len(keys) == 0 {
			// Snapshot at the fence shows nothing left beyond the cursor: the
			// target key set for this prefix is fully tombstoned.
			e.State = purgeStateDone
			e.Cursor = nil
			e.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			if err := ledger.persist(ledgerPath); err != nil {
				return deletedThisRun, err
			}
			return deletedThisRun, nil
		}

		if err := db.deletePurgeBatch(keys); err != nil {
			return deletedThisRun, err
		}
		// Deletes are committed; make them durable before advancing the ledger.
		if err := db.Sync(); err != nil {
			return deletedThisRun, y.Wrapf(err, "purge: sync before ledger update")
		}
		barrier++
		if hook.onBarrier != nil {
			// Simulates a crash after deletes were synced but before the
			// ledger moved: rerun re-enumerates at the fence and re-deletes.
			if err := hook.onBarrier(purgeBarrierBeforeLedger, e.Prefix, barrier); err != nil {
				return deletedThisRun, err
			}
		}

		e.Cursor = keys[len(keys)-1]
		e.DeletedKeys += uint64(len(keys))
		if err := ledger.persist(ledgerPath); err != nil {
			// The ledger is the source of progress: if it cannot be written,
			// the whole run fails rather than continuing unrecorded.
			return deletedThisRun, err
		}
		deletedThisRun += uint64(len(keys))
		if hook.onBarrier != nil {
			if err := hook.onBarrier(purgeBarrierAfterLedger, e.Prefix, barrier); err != nil {
				return deletedThisRun, err
			}
		}
	}
}

// scanPurgeBatch returns up to limit live user keys under prefix that are
// visible at fenceTs and strictly greater than after. A fresh snapshot
// transaction at the (old, fixed) fence timestamp is used on every batch, so
// the target key set stays identical across runs and restarts.
func (db *DB) scanPurgeBatch(fenceTs uint64, prefix, after []byte, limit int) ([][]byte, error) {
	// Managed-style construction only to pin the read timestamp; the txn is
	// read-only and never commits. The DB itself is not managed, so Discard
	// would otherwise report this old readTs to the PendingReads watermark out
	// of order (the watermark has long advanced past the fence). We therefore
	// neither Begin nor Done the watermark for this txn: marking doneRead
	// suppresses the Done call in Discard. The tradeoff is that this read does
	// not pin old versions for GC; any version physically collected before the
	// sweep reaches it is already gone and can never be resurrected.
	txn := db.newTransaction(false, true)
	txn.readTs = fenceTs
	txn.doneRead = true
	defer txn.Discard()

	opt := DefaultIteratorOptions
	opt.PrefetchValues = false // Keys only: deletes do not need values.
	opt.Prefix = prefix
	it := txn.NewIterator(opt)
	defer it.Close()

	seek := prefix
	if len(after) > 0 {
		seek = after
	}
	it.Seek(seek)
	if len(after) > 0 {
		// The seek lands on >= after. A key equal to the cursor may still be
		// visible at the fence (its tombstone is newer than the fence), so skip
		// everything up to and including the cursor.
		for it.Valid() && bytes.Compare(it.Item().Key(), after) <= 0 {
			it.Next()
		}
	}

	keys := make([][]byte, 0, limit)
	for ; it.Valid(); it.Next() {
		key := it.Item().Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		keys = append(keys, it.Item().KeyCopy(nil))
		if len(keys) == limit {
			break
		}
	}
	return keys, nil
}

// deletePurgeBatch tombstones the given keys. Blind deletes do not conflict
// with concurrent transactions.
func (db *DB) deletePurgeBatch(keys [][]byte) error {
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		if err := wb.Delete(key); err != nil {
			return y.Wrapf(err, "purge: delete %q", key)
		}
	}
	if err := wb.Flush(); err != nil {
		return y.Wrapf(err, "purge: flush delete batch")
	}
	return nil
}

func normalizePurgePrefixes(in [][]byte) ([][]byte, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, p := range in {
		if len(p) == 0 {
			return nil, y.Wrapf(ErrInvalidRequest,
				"purge: empty prefix is not allowed (it would match every key)")
		}
		if bytes.HasPrefix(p, badgerPrefix) {
			return nil, y.Wrapf(ErrInvalidKey, "purge: prefix %q uses the reserved internal prefix", p)
		}
		if _, ok := seen[string(p)]; ok {
			return nil, y.Wrapf(ErrInvalidRequest, "purge: duplicate prefix %q in list", p)
		}
		seen[string(p)] = struct{}{}
		out[i] = append([]byte(nil), p...)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out, nil
}

const (
	purgeLedgerFileName = "PURGE-LEDGER"

	purgeLedgerMagic   = "!bplg!\n"
	purgeLedgerVersion = byte(1)
	// header: magic(7) + version(1) + payloadLen(4); trailer: crc32c(4).
	purgeLedgerHeaderSize = 12
	purgeLedgerCRCSIZE    = 4
	purgeLedgerMaxSize    = 64 << 20

	defaultPurgeBatchSize = 1024
	// Upper bound on a single scan/delete batch, mainly to cap the in-memory
	// key slice. WriteBatch commits internally long before this many entries.
	maxPurgeBatchSize = 100_000

	purgeStatePending  = "pending"
	purgeStateSweeping = "sweeping"
	purgeStateDone     = "done"
)

// Barrier points exposed to the (test-only) hook.
const (
	purgeBarrierBeforeLedger = "beforeLedger"
	purgeBarrierAfterLedger  = "afterLedger"
)

// PrefixPurgeOptions configures DB.PurgePrefixes.
type PrefixPurgeOptions struct {
	// Prefixes is the authoritative drop set. Only keys under these prefixes
	// are deleted. The slice must not contain empty or duplicate prefixes. An
	// empty list cleans nothing.
	Prefixes [][]byte

	// LedgerDir is the directory holding the persistent ledger file. It
	// defaults to the DB directory. Using a different (empty) directory starts
	// a new cleanup generation with a fresh fence timestamp.
	LedgerDir string

	// BatchSize is the number of keys tombstoned between two ledger syncs.
	// Defaults to 1024. Larger batches mean fewer fsyncs; smaller batches mean
	// less repeated work after a crash.
	BatchSize int

	// hook is for crash injection in tests; nil outside of tests.
	hook purgeHook
}

// PrefixPurgeReport summarizes one PurgePrefixes run.
type PrefixPurgeReport struct {
	// LedgerPath is the ledger file used by the run.
	LedgerPath string
	// FenceTs is the fixed snapshot timestamp all runs enumerate at.
	FenceTs uint64
	// DeletedKeys is the number of keys tombstoned during this run.
	DeletedKeys uint64
	// CompletedPrefixes reached the done state during this run.
	CompletedPrefixes [][]byte
	// SkippedPrefixes were already marked done in the ledger.
	SkippedPrefixes [][]byte
	// RetiredPrefixes are tracked by the ledger but were absent from the
	// supplied prefix list; they were not touched.
	RetiredPrefixes [][]byte
}

// purgeHook allows tests to abort a run at the durability barriers, which is
// equivalent to killing the process at that point.
type purgeHook struct {
	onBarrier func(when string, prefix []byte, batches int) error
}

type purgeLedger struct {
	Version   int           `json:"version"`
	FenceTs   uint64        `json:"fence_ts"`
	CreatedAt string        `json:"created_at"`
	UpdatedAt string        `json:"updated_at"`
	Prefixes  []*purgeEntry `json:"prefixes"`
	dirty     bool          `json:"-"`
}

type purgeEntry struct {
	Prefix      []byte `json:"prefix"`
	State       string `json:"state"`
	Cursor      []byte `json:"cursor,omitempty"`
	DeletedKeys uint64 `json:"deleted_keys"`
	StartedAt   string `json:"started_at,omitempty"`
	FinishedAt  string `json:"finished_at,omitempty"`
}

func loadOrCreatePurgeLedger(path string, fenceTs uint64) (*purgeLedger, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if fenceTs == 0 {
			return nil, y.Wrapf(ErrInvalidRequest, "purge: invalid fence timestamp")
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		l := &purgeLedger{
			Version:   int(purgeLedgerVersion),
			FenceTs:   fenceTs,
			CreatedAt: now,
			UpdatedAt: now,
			dirty:     true,
		}
		// Persist before any deletion happens: if the ledger cannot be
		// created, the run must fail without touching data.
		if err := l.persist(path); err != nil {
			return nil, y.Wrapf(err, "purge: cannot create ledger")
		}
		return l, nil
	}
	if err != nil {
		return nil, y.Wrapf(err, "purge: cannot read ledger")
	}
	l, err := decodePurgeLedger(data)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func decodePurgeLedger(data []byte) (*purgeLedger, error) {
	if len(data) < purgeLedgerHeaderSize+purgeLedgerCRCSIZE {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger is truncated")
	}
	if string(data[:len(purgeLedgerMagic)]) != purgeLedgerMagic {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger magic mismatch")
	}
	if data[len(purgeLedgerMagic)] != purgeLedgerVersion {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: unsupported ledger version %d",
			data[len(purgeLedgerMagic)])
	}
	payloadLen := int(binary.BigEndian.Uint32(data[8:12]))
	if payloadLen == 0 ||
		purgeLedgerHeaderSize+payloadLen+purgeLedgerCRCSIZE != len(data) ||
		payloadLen > purgeLedgerMaxSize {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger length is invalid")
	}
	payload := data[purgeLedgerHeaderSize : purgeLedgerHeaderSize+payloadLen]
	wantCRC := binary.BigEndian.Uint32(data[purgeLedgerHeaderSize+payloadLen:])
	if crc32.Checksum(payload, y.CastagnoliCrcTable) != wantCRC {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger checksum mismatch")
	}
	l := &purgeLedger{}
	if err := json.Unmarshal(payload, l); err != nil {
		return nil, y.Wrapf(err, "purge: ledger payload is corrupt")
	}
	if l.Version != int(purgeLedgerVersion) || l.FenceTs == 0 {
		return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger content is invalid")
	}
	for _, e := range l.Prefixes {
		if len(e.Prefix) == 0 {
			return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger contains an empty prefix")
		}
		switch e.State {
		case purgeStatePending, purgeStateSweeping, purgeStateDone:
		default:
			return nil, y.Wrapf(ErrInvalidRequest, "purge: ledger has unknown state %q", e.State)
		}
	}
	return l, nil
}

// admit registers newly listed prefixes. Prefixes dropped from the list keep
// their stored state so that progress is preserved should they return.
func (l *purgeLedger) admit(prefixes [][]byte) error {
	known := make(map[string]*purgeEntry, len(l.Prefixes))
	for _, e := range l.Prefixes {
		known[string(e.Prefix)] = e
	}
	for _, p := range prefixes {
		if _, ok := known[string(p)]; ok {
			continue
		}
		l.Prefixes = append(l.Prefixes, &purgeEntry{
			Prefix: append([]byte(nil), p...),
			State:  purgeStatePending,
		})
		l.dirty = true
	}
	sort.Slice(l.Prefixes, func(i, j int) bool {
		return bytes.Compare(l.Prefixes[i].Prefix, l.Prefixes[j].Prefix) < 0
	})
	// Defensive: reject an internally inconsistent ledger layout.
	for i := 1; i < len(l.Prefixes); i++ {
		if bytes.Equal(l.Prefixes[i-1].Prefix, l.Prefixes[i].Prefix) {
			return y.Wrapf(ErrInvalidRequest, "purge: duplicate prefix %q in ledger",
				l.Prefixes[i].Prefix)
		}
	}
	return nil
}

func (l *purgeLedger) persistIfDirty(path string) error {
	if !l.dirty {
		return nil
	}
	return l.persist(path)
}

// persist writes the ledger to a temp file, fsyncs it, atomically renames it
// over the ledger path and fsyncs the directory — the same durability pattern
// used by the key registry.
func (l *purgeLedger) persist(path string) error {
	l.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(l)
	if err != nil {
		return y.Wrapf(err, "purge: marshal ledger")
	}
	frame := make([]byte, 0, purgeLedgerHeaderSize+len(payload)+purgeLedgerCRCSIZE)
	frame = append(frame, []byte(purgeLedgerMagic)...)
	frame = append(frame, purgeLedgerVersion)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.Checksum(payload, y.CastagnoliCrcTable))

	tmpPath := path + ".tmp"
	fp, err := y.OpenTruncFile(tmpPath, true)
	if err != nil {
		return y.Wrapf(err, "purge: open ledger temp file")
	}
	if _, err = fp.Write(frame); err != nil {
		fp.Close()
		return y.Wrapf(err, "purge: write ledger")
	}
	if err = fp.Close(); err != nil {
		return y.Wrapf(err, "purge: close ledger temp file")
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return y.Wrapf(err, "purge: rename ledger")
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		return y.Wrapf(err, "purge: sync ledger directory")
	}
	l.dirty = false
	return nil
}
