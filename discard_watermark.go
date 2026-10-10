/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// In managed mode the discard timestamp (oracle.discardTs) used to live only in
// memory: after a restart it reset to zero, so invalid/deleted versions —
// especially versions settled in Lmax tables, which background compaction only
// rewrites when they are more than an hour old and carry at least 10MB of stale
// data — kept occupying space indefinitely. This file makes the applied
// discard watermark durable:
//
//   - On Open, the last applied watermark is restored before compactions start.
//   - ReconcileDiscardTs compares the caller's authoritative watermark against
//     the durable record, refuses to regress or to act on an unreadable input,
//     and records each newly applied watermark exactly once.
//
// Record layout of the DISCARD-WATERMARK file:
//
//	+-----------------+----------------------+----------------+
//	| magic (8 bytes) | watermark (uint64 BE)| crc32 (uint32) |
//	+-----------------+----------------------+----------------+
//
// The record is 20 bytes. The checksum covers the first 16 bytes. Updates are
// written to a temporary file, fsynced, atomically renamed, and followed by a
// directory fsync, so a crash can leave either the old record or the new one,
// never a torn write.
const (
	discardWatermarkFname    = "DISCARD-WATERMARK"
	discardWatermarkTmpFname = "DISCARD-WATERMARK-TMP"

	discardWatermarkMagic0 = 'B'
	discardWatermarkMagic1 = 'D'
	discardWatermarkMagic2 = 'G'
	discardWatermarkMagic3 = 'R'
	discardWatermarkMagic4 = 'D'
	discardWatermarkMagic5 = 'W'
	discardWatermarkMagic6 = 'T'
	discardWatermarkMagic7 = 'S'

	// magic(8) + watermark(8) + crc32(4)
	discardWatermarkRecordSize = 20
)

var discardWatermarkMagic = [8]byte{
	discardWatermarkMagic0, discardWatermarkMagic1,
	discardWatermarkMagic2, discardWatermarkMagic3,
	discardWatermarkMagic4, discardWatermarkMagic5,
	discardWatermarkMagic6, discardWatermarkMagic7,
}

// loadPersistedDiscardTs reads the durable discard-watermark record from dir.
// If no record exists yet, it returns (0, false, nil). A record that exists but
// cannot be decoded or verified yields an error: callers must fail closed in
// that case rather than assuming a watermark of zero.
func loadPersistedDiscardTs(dir string) (uint64, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, discardWatermarkFname))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, y.Wrapf(err, "while reading %s", discardWatermarkFname)
	}
	if len(data) != discardWatermarkRecordSize {
		return 0, false, fmt.Errorf("%s has size %d, want %d bytes: %w",
			discardWatermarkFname, len(data), discardWatermarkRecordSize,
			ErrInvalidDiscardWatermark)
	}
	var magic [8]byte
	copy(magic[:], data[:8])
	if magic != discardWatermarkMagic {
		return 0, false, fmt.Errorf("%s has an invalid magic header: %w",
			discardWatermarkFname, ErrInvalidDiscardWatermark)
	}
	storedCRC := binary.BigEndian.Uint32(data[16:20])
	if crc32.Checksum(data[:16], y.CastagnoliCrcTable) != storedCRC {
		return 0, false, fmt.Errorf("%s checksum mismatch: %w",
			discardWatermarkFname, ErrInvalidDiscardWatermark)
	}
	return binary.BigEndian.Uint64(data[8:16]), true, nil
}

// persistDiscardTs durably records ts as the applied discard watermark. The
// write is atomic with respect to crashes (temp file + fsync + rename +
// directory fsync).
func persistDiscardTs(dir string, ts uint64) error {
	buf := make([]byte, discardWatermarkRecordSize)
	copy(buf[:8], discardWatermarkMagic[:])
	binary.BigEndian.PutUint64(buf[8:16], ts)
	binary.BigEndian.PutUint32(buf[16:20],
		crc32.Checksum(buf[:16], y.CastagnoliCrcTable))

	tmpPath := filepath.Join(dir, discardWatermarkTmpFname)
	fp, err := y.OpenTruncFile(tmpPath, true)
	if err != nil {
		return y.Wrapf(err, "while creating %s", discardWatermarkTmpFname)
	}
	if _, err = fp.Write(buf); err != nil {
		_ = fp.Close()
		return y.Wrapf(err, "while writing %s", discardWatermarkTmpFname)
	}
	// Close (and flush) before renaming; Windows requires the file to be closed
	// before renaming.
	if err = fp.Close(); err != nil {
		return y.Wrapf(err, "while closing %s", discardWatermarkTmpFname)
	}
	if err = os.Rename(tmpPath, filepath.Join(dir, discardWatermarkFname)); err != nil {
		return y.Wrapf(err, "while renaming %s", discardWatermarkTmpFname)
	}
	return syncDir(dir)
}

// readAuthoritativeDiscardTs reads the caller-maintained authoritative
// watermark. The file must contain a single decimal uint64; surrounding
// whitespace and a trailing newline are allowed. Anything else — a missing
// file, an empty file, a sign, non-decimal content, an overflow — is an error.
func readAuthoritativeDiscardTs(path string) (uint64, error) {
	if path == "" {
		return 0, y.Wrap(ErrInvalidRequest, "authoritative discard watermark file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, y.Wrapf(err, "while reading authoritative discard watermark %q", path)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, y.Wrapf(ErrInvalidRequest,
			"authoritative discard watermark %q is empty", path)
	}
	ts, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, y.Wrapf(err,
			"while parsing authoritative discard watermark %q", path)
	}
	return ts, nil
}

// ReconcileDiscardTsOptions controls DB.ReconcileDiscardTs.
type ReconcileDiscardTsOptions struct {
	// WatermarkFile is the path of the file, maintained outside Badger, that
	// holds the authoritative discard watermark as a single decimal uint64
	// (surrounding whitespace/newlines are allowed). Required.
	WatermarkFile string

	// Reclaim, when true, does not just advance the watermark but also forces
	// physical reclamation in the same call: all SST tables are compacted down
	// to the last level, an Lmax-to-Lmax compaction drops every version made
	// invalid by the watermark (bypassing the background compaction's one-hour
	// age and 10MB stale-data thresholds), and value-log GC is repeated until no
	// log file qualifies anymore. Reclamation itself is idempotent: with the
	// watermark unchanged a rerun performs only the unavoidable rewrites (and
	// none at all once the tree has converged).
	//
	// When false, only the durable watermark state is reconciled; versions are
	// reclaimed later by ordinary compactions as they pick the tables up.
	Reclaim bool

	// ValueLogGCDiscardRatio is passed to RunValueLogGC during Reclaim. It must
	// be in (0.0, 1.0); zero selects the default of 0.5.
	ValueLogGCDiscardRatio float64
}

// ReconcileDiscardTs reconciles the DB's discard watermark with the
// authoritative watermark in opts.WatermarkFile and returns the watermark now
// in effect.
//
// It is meant for Badger running in managed mode, where the caller decides up
// to which timestamp no snapshot read will ever happen again. The rules are:
//
//   - If the authoritative file cannot be read or parsed, nothing is changed.
//   - If the durable record of the watermark already applied cannot be read,
//     nothing is changed.
//   - If the authoritative watermark is below the watermark already applied and
//     recorded (a rollback), ErrDiscardTsRegressed is returned and nothing is
//     changed: the in-memory watermark is never lowered and the record is not
//     rewritten.
//   - A watermark that has already been applied is not processed again: an
//     equal authoritative watermark is a no-op (unless Reclaim requests
//     physical reclamation).
//   - A newer watermark is recorded durably BEFORE it takes effect, so an
//     Open after a crash restores it automatically; versions strictly above
//     the watermark are always retained (the discard logic in compaction only
//     drops versions at or below it, and never the only live versions).
//   - Repeating the call converges: once the watermark is applied and the
//     invalid versions are reclaimed, further identical calls do nothing.
func (db *DB) ReconcileDiscardTs(opts ReconcileDiscardTsOptions) (uint64, error) {
	if !db.opt.managedTxns {
		panic("Cannot use ReconcileDiscardTs with managedDB=false.")
	}
	if db.opt.InMemory {
		return 0, y.Wrap(ErrInvalidRequest,
			"ReconcileDiscardTs is not supported in InMemory mode")
	}
	if db.opt.ReadOnly {
		return 0, y.Wrap(ErrInvalidRequest,
			"ReconcileDiscardTs is not supported in read-only mode")
	}
	if db.IsClosed() {
		return 0, ErrDBClosed
	}
	if opts.ValueLogGCDiscardRatio < 0 || opts.ValueLogGCDiscardRatio >= 1 {
		return 0, y.Wrapf(ErrInvalidRequest,
			"invalid ValueLogGCDiscardRatio %v, must be within (0.0, 1.0)",
			opts.ValueLogGCDiscardRatio)
	}

	// Serialize reconciliations against each other. The maintenance window
	// inside reclamation stops the background compactors, so concurrent
	// reconciles would interfere with one another.
	db.discardWMMu.Lock()
	defer db.discardWMMu.Unlock()

	// Fail closed on an unreadable authoritative input.
	authority, err := readAuthoritativeDiscardTs(opts.WatermarkFile)
	if err != nil {
		return 0, err
	}

	// Fail closed on an unreadable record of what has already been applied.
	applied, _, err := loadPersistedDiscardTs(db.opt.Dir)
	if err != nil {
		return 0, err
	}

	// A rollback of the authority relative to what this DB has already applied
	// and acted on is unsafe: refuse to do anything.
	if authority < applied {
		return db.orc.discardAtOrBelow(), fmt.Errorf(
			"authoritative watermark %d is below applied watermark %d: %w",
			authority, applied, ErrDiscardTsRegressed)
	}

	// A direct SetDiscardTs call earlier in this process may have raised the
	// in-memory watermark above the durable record. Never record less than the
	// DB may already have acted on; it would make the rollback guard accept an
	// authority value below versions that were physically discarded.
	target := authority
	if inMemory := db.orc.discardAtOrBelow(); inMemory > target {
		target = inMemory
	}

	switch {
	case target > applied:
		// Persist first. If we crash after this write but before returning,
		// Open restores the watermark from the record; the next reconciliation
		// with the same authority observes equality and simply converges.
		if err := persistDiscardTs(db.opt.Dir, target); err != nil {
			return 0, err
		}
		db.SetDiscardTs(target)
		db.opt.Infof("ReconcileDiscardTs advanced applied watermark %d -> %d\n",
			applied, target)

	default:
		// target == applied: this watermark has already been processed. The
		// only possible work here is restoring the in-memory value, e.g. if
		// the record was loaded by a build that skipped the Open-time restore.
		if db.orc.discardAtOrBelow() < applied {
			db.SetDiscardTs(applied)
		}
		if !opts.Reclaim {
			db.opt.Debugf("ReconcileDiscardTs: watermark %d already applied; nothing to do\n",
				applied)
		}
	}

	if opts.Reclaim {
		if err := db.reclaimBelowDiscardTs(opts.ValueLogGCDiscardRatio); err != nil {
			return target, err
		}
	}
	return target, nil
}

// reconcileCompactorID identifies compactions driven by reconciliation in the
// logs (Flatten uses 175).
const reconcileCompactorID = 176

// reclaimBelowDiscardTs forces physical reclamation of versions invalidated by
// the current in-memory discard watermark. It mirrors Flatten's maintenance
// window: background compactors are stopped for the duration and restarted
// afterwards (also on error).
func (db *DB) reclaimBelowDiscardTs(gcRatio float64) error {
	db.stopCompactions()
	defer db.startCompactions()

	lastLevel := db.opt.MaxLevels - 1

	// With a zero watermark no version is eligible for dropping, so forcing LSM
	// rewrites would just churn the tree. Value-log GC can still be useful on
	// its own (it works off the accumulated discard stats).
	if db.orc.discardAtOrBelow() > 0 {
		if err := db.compactAllToLmax(lastLevel); err != nil {
			return err
		}
		if err := db.rewriteLmaxForDiscard(lastLevel); err != nil {
			return err
		}
	}

	// Reclaim value-log space whose LSM references were just dropped. The
	// compactions above fed the discard stats; keep rewriting qualifying log
	// files until a GC pass finds no candidate (fixed point). Each successful
	// rewrite removes one log file, so the loop is bounded.
	ratio := gcRatio
	if ratio == 0 {
		ratio = 0.5
	}
	const maxValueLogGCRounds = 1 << 16
	for round := 0; round < maxValueLogGCRounds; round++ {
		err := db.RunValueLogGC(ratio)
		if errors.Is(err, ErrNoRewrite) {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// compactAllToLmax fans every SST into the last level by repeatedly compacting
// the lowest non-empty level. The caller must have stopped the background
// compactors. Every rewrite applies the current discard watermark.
func (db *DB) compactAllToLmax(lastLevel int) error {
	for {
		lowest := -1
		for l := 0; l < lastLevel; l++ {
			if db.lc.levels[l].numTables() > 0 {
				lowest = l
				break
			}
		}
		if lowest < 0 {
			return nil
		}
		err := db.lc.doCompact(reconcileCompactorID,
			compactionPriority{level: lowest, score: 1.71})
		if errors.Is(err, errFillTables) {
			// Nothing pickable right now (e.g. concurrent writes keep the
			// ranges busy). Background compactors, restarted on return, take
			// over; another Reclaim run converges what remains.
			db.opt.Warningf("ReconcileDiscardTs: could not compact level %d; "+
				"leaving it to background compaction\n", lowest)
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// rewriteLmaxForDiscard performs one Lmax-to-Lmax rewrite of the whole level.
// Background compaction only schedules such a rewrite for tables older than an
// hour carrying >=10MB of stale data, which is why long-expired versions pile
// up across restarts. A single forced pass physically drops every droppable
// version at or below the watermark; versions above it are retained by the
// compaction logic. The caller must have stopped the background compactors.
func (db *DB) rewriteLmaxForDiscard(lastLevel int) error {
	lmax := db.lc.levels[lastLevel]
	lmax.RLock()
	tables := make([]*table.Table, len(lmax.tables))
	copy(tables, lmax.tables)
	lmax.RUnlock()

	if len(tables) == 0 {
		return nil
	}
	// As in fillMaxLevelTables, an Lmax-to-Lmax def takes exactly one table as
	// top and the remaining (key-adjacent) tables as bot. Tables of a non-zero
	// level are stored in key order, so top = first and bot = the rest covers
	// the whole level in one contiguous rewrite.
	cd := compactDef{
		thisLevel: lmax,
		nextLevel: lmax,
		top:       tables[:1],
		bot:       tables[1:],
		t:         db.lc.levelTargets(),
	}
	return db.lc.runCompactDef(reconcileCompactorID, lastLevel, cd)
}
