/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/y"
)

// FLATTEN-LEDGER is the durable migration ledger used by DB.Flatten.
//
// A flatten run merges every SST table into one level. It can run for hours and
// a process kill or machine restart historically left the operator with no way
// to tell which tables had already moved, so re-running flatten started the
// whole expensive operation from scratch.
//
// The ledger records the flatten job as an append-only sequence of framed JSON
// records:
//
//   - JobStart declares the job (id, target level, and the file-id fence that
//     distinguishes tables existing at job start from tables born during the
//     run).
//   - BatchPrepare is fsynced *before* the main MANIFEST commits a compaction
//     batch; BatchCommit is fsynced *after* it committed. Input table ids (with
//     their levels) and output table ids are recorded, giving per-table
//     from/to/progress lineage across the hops a table takes toward the target
//     level.
//   - TableBorn registers a table whose id is >= the fence (a memtable flush
//     that landed while flatten was running).
//   - BatchAbort/JobComplete close a reconciled batch and the whole job.
//
// The ledger never decides table state by itself -- the main MANIFEST remains
// the single source of truth. A compaction commits to MANIFEST atomically (all
// output CREATEs and input DELETEs in one fsynced change set), so after a crash
// a prepared-but-not-committed batch is reconciled deterministically against
// the replayed MANIFEST: if the outputs live at the destination the batch is
// retroactively committed, otherwise it is aborted and its (still live) inputs
// are moved again. Committed batches can never move a table twice because their
// input ids no longer exist in the MANIFEST and can't be picked again.
//
// On-disk layout:
//
//	+---------------------------+--------------------------------------------+
//	| magic "FLTN" (4)          | version u16 | reserved u16                 |
//	+---------------------------+--------------------------------------------+
//	| repeated frames: u32 BE length | u32 BE CRC32C(payload) | JSON payload |
//	+------------------------------------------------------------------------+
//
// Like MANIFEST, a torn tail (partial frame or checksum mismatch after a crash
// mid-append) is truncated to the last full frame on open. Payloads are JSON on
// purpose, so operators can inspect migration progress with standard tools.
// The ledger contains only table ids, levels and batch numbers -- never user
// data or key material.
const (
	flattenLedgerFilename = "FLATTEN-LEDGER"

	flattenLedgerRecordJobStart     = "JobStart"
	flattenLedgerRecordBatchPrepare = "BatchPrepare"
	flattenLedgerRecordBatchCommit  = "BatchCommit"
	flattenLedgerRecordBatchAbort   = "BatchAbort"
	flattenLedgerRecordTableBorn    = "TableBorn"
	flattenLedgerRecordJobComplete  = "JobComplete"
)

// flattenLedgerMagicText has to be 4 bytes and can never change.
var flattenLedgerMagicText = [4]byte{'F', 'L', 'T', 'N'}

// flattenLedgerMagicVersion is allocated 2 bytes, so its value must stay <= math.MaxUint16.
const flattenLedgerMagicVersion uint16 = 1

var (
	errFlattenLedgerBadMagic     = errors.New("flatten ledger has bad magic")
	errFlattenLedgerBadVersion   = errors.New("flatten ledger has unsupported version")
	errFlattenLedgerBadChecksum  = errors.New("flatten ledger has checksum mismatch")
	errFlattenLedgerInconsistent = errors.New("flatten ledger is inconsistent with MANIFEST")
	errFlattenLedgerBroken       = errors.New("flatten ledger has failed writes and cannot complete the job")
)

// flattenSyncFunc is a package-level indirection so tests can inject sync
// failures. Production code uses an ordinary fdatasync/fsync.
var flattenSyncFunc = func(f *os.File) error { return f.Sync() }

// ledgerTableRef identifies a table and the level it sits at.
type ledgerTableRef struct {
	ID    uint64 `json:"id"`
	Level int    `json:"level"`
}

// flattenLedgerRecord is the JSON payload of one ledger frame. Only the fields
// relevant for a given record Type are populated.
type flattenLedgerRecord struct {
	Type       string           `json:"type"`
	JobID      uint64           `json:"job_id,omitempty"`
	BatchSeq   uint64           `json:"batch_seq,omitempty"`
	FromLevel  int              `json:"from_level,omitempty"`
	ToLevel    int              `json:"to_level,omitempty"`
	Inputs     []ledgerTableRef `json:"inputs,omitempty"`
	Outputs    []uint64         `json:"outputs,omitempty"`
	TableID    uint64           `json:"table_id,omitempty"`
	Target     int              `json:"target_level,omitempty"`
	Fence      uint64           `json:"fence_next_file_id,omitempty"`
	FinalLevel int              `json:"final_level,omitempty"`
	Reason     string           `json:"reason,omitempty"`
	CreatedAt  int64            `json:"created_at_unix_nano,omitempty"`
}

// flattenBatch is the in-memory view of one BatchPrepare record and its
// resolution (committed to MANIFEST or aborted during recovery).
type flattenBatch struct {
	seq                uint64
	fromLevel, toLevel int
	inputs             []ledgerTableRef
	outputs            []uint64
	committed          bool
	aborted            bool
}

// flattenLedger is an open FLATTEN-LEDGER file plus the replayed job state.
type flattenLedger struct {
	fp  *os.File
	dir string

	// Guards file appends, fp, and all state below. Lock ordering: this lock may
	// be held while taking the MANIFEST append lock (e.g. bornAround wraps a
	// MANIFEST create); the MANIFEST lock is never held while taking this one.
	mu sync.Mutex

	jobID       uint64
	targetLevel int
	fence       uint64
	complete    bool
	resume      bool

	// batches is indexed by batch seq minus one; seq values are gapless.
	batches  []*flattenBatch
	prepared map[uint64]*flattenBatch
	// knownTableIDs contains every id ever referenced by a batch input/output or
	// by a TableBorn record. The completion gate uses it to prove that no table
	// born during the job escaped the ledger.
	knownTableIDs map[uint64]struct{}

	// broken is set once an append/sync fails. The job can no longer be trusted
	// to complete and every subsequent operation fails fast; the on-disk ledger
	// is left intact for the next run to reconcile.
	broken bool
}

// flattenLedgerTestHook, when non-nil, is invoked before every ledger frame is
// written. Returning an error aborts the append before any byte hits disk; the
// compaction batch then rolls back with MANIFEST untouched. Test-only seam,
// same role as the package-level syncFunc.
var flattenLedgerTestHook func(rec flattenLedgerRecord) error

func flattenLedgerPath(dir string) string {
	return filepath.Join(dir, flattenLedgerFilename)
}

func flattenLedgerDonePath(dir string, jobID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%d-DONE", flattenLedgerFilename, jobID))
}

// createFlattenLedger creates a brand-new ledger file with the header and
// returns it open at end of file.
func createFlattenLedger(dir string) (*flattenLedger, error) {
	path := flattenLedgerPath(dir)
	fp, err := y.OpenTruncFile(path, false) // We explicitly sync below.
	if err != nil {
		return nil, err
	}
	if _, err := fp.Write(flattenLedgerHeader()); err != nil {
		_ = fp.Close()
		return nil, err
	}
	if err := flattenSyncFunc(fp); err != nil {
		_ = fp.Close()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return &flattenLedger{
		fp:            fp,
		dir:           dir,
		prepared:      make(map[uint64]*flattenBatch),
		knownTableIDs: make(map[uint64]struct{}),
	}, nil
}

func flattenLedgerHeader() []byte {
	buf := make([]byte, 8)
	copy(buf[0:4], flattenLedgerMagicText[:])
	binary.BigEndian.PutUint16(buf[4:6], flattenLedgerMagicVersion)
	// bytes 6:8 stay zero (reserved).
	return buf
}

// openFlattenLedger opens an existing ledger, validates the header, replays all
// frames into a fresh state and truncates a torn tail. It returns (nil, nil) if
// no ledger file exists.
func openFlattenLedger(dir string) (*flattenLedger, error) {
	path := flattenLedgerPath(dir)
	fp, err := y.OpenExistingFile(path, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	lg := &flattenLedger{
		fp:            fp,
		dir:           dir,
		prepared:      make(map[uint64]*flattenBatch),
		knownTableIDs: make(map[uint64]struct{}),
	}
	goodOffset, err := lg.replay(true /* truncateTail */)
	if err != nil {
		_ = fp.Close()
		return nil, err
	}
	if err := fp.Truncate(goodOffset); err != nil {
		_ = fp.Close()
		return nil, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return lg, nil
}

// readFlattenLedger replays the ledger without opening it for writing and
// without truncating anything. Used by FlattenStatus while a job could be live.
func readFlattenLedger(dir string) (*flattenLedger, error) {
	path := flattenLedgerPath(dir)
	fp, err := y.OpenExistingFile(path, y.ReadOnly)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	lg := &flattenLedger{
		fp:            fp,
		dir:           dir,
		prepared:      make(map[uint64]*flattenBatch),
		knownTableIDs: make(map[uint64]struct{}),
	}
	if _, err := lg.replay(false /* truncateTail */); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return lg, nil
}

// replay reads the header and all frames, applying each record to lg. It returns
// the offset immediately after the last fully valid frame (the position a torn
// tail must be truncated to).
func (lg *flattenLedger) replay(truncateTail bool) (int64, error) {
	var header [8]byte
	if _, err := io.ReadFull(lg.fp, header[:]); err != nil {
		return 0, errFlattenLedgerBadMagic
	}
	if string(header[0:4]) != string(flattenLedgerMagicText[:]) {
		return 0, errFlattenLedgerBadMagic
	}
	if version := binary.BigEndian.Uint16(header[4:6]); version != flattenLedgerMagicVersion {
		return 0, fmt.Errorf("%w: %d (we support %d)",
			errFlattenLedgerBadVersion, version, flattenLedgerMagicVersion)
	}

	stat, err := lg.fp.Stat()
	if err != nil {
		return 0, err
	}

	goodOffset := int64(8)
	var offset int64 = 8
	for {
		var hdr [8]byte
		n, err := io.ReadFull(lg.fp, hdr[:])
		offset += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // No (full) frame header left: clean EOF or torn append.
		}
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		if uint64(length)+uint64(offset) > uint64(stat.Size()) {
			// Frame payload was not fully written.
			break
		}
		payload := make([]byte, length)
		n, err = io.ReadFull(lg.fp, payload)
		offset += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, err
		}
		if crc32.Checksum(payload, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(hdr[4:8]) {
			// A frame past this point can't be trusted; stop at the last good
			// frame, exactly like MANIFEST replay does.
			break
		}
		var rec flattenLedgerRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}
		if err := lg.apply(rec); err != nil {
			return 0, err
		}
		goodOffset = offset
	}
	return goodOffset, nil
}

// apply folds one replayed record into the in-memory state.
func (lg *flattenLedger) apply(rec flattenLedgerRecord) error {
	switch rec.Type {
	case flattenLedgerRecordJobStart:
		lg.jobID = rec.JobID
		lg.targetLevel = rec.Target
		lg.fence = rec.Fence
	case flattenLedgerRecordBatchPrepare:
		if rec.BatchSeq != uint64(len(lg.batches))+1 {
			return fmt.Errorf("%w: batch seq gap, expected %d got %d",
				errFlattenLedgerInconsistent, len(lg.batches)+1, rec.BatchSeq)
		}
		b := &flattenBatch{
			seq:       rec.BatchSeq,
			fromLevel: rec.FromLevel,
			toLevel:   rec.ToLevel,
			inputs:    rec.Inputs,
			outputs:   rec.Outputs,
		}
		lg.batches = append(lg.batches, b)
		lg.prepared[b.seq] = b
		lg.rememberRefs(b)
	case flattenLedgerRecordBatchCommit:
		b, ok := lg.prepared[rec.BatchSeq]
		if !ok {
			// A commit without prepare means the ledger itself is corrupt rather
			// than merely torn.
			return fmt.Errorf("%w: commit for unknown batch %d",
				errFlattenLedgerInconsistent, rec.BatchSeq)
		}
		delete(lg.prepared, b.seq)
		b.committed = true
	case flattenLedgerRecordBatchAbort:
		b, ok := lg.prepared[rec.BatchSeq]
		if !ok {
			return fmt.Errorf("%w: abort for unknown batch %d",
				errFlattenLedgerInconsistent, rec.BatchSeq)
		}
		delete(lg.prepared, b.seq)
		b.aborted = true
	case flattenLedgerRecordTableBorn:
		lg.knownTableIDs[rec.TableID] = struct{}{}
	case flattenLedgerRecordJobComplete:
		lg.complete = true
	default:
		// Unknown record types from a newer version are skipped defensively.
	}
	return nil
}

func (lg *flattenLedger) rememberRefs(b *flattenBatch) {
	for _, in := range b.inputs {
		lg.knownTableIDs[in.ID] = struct{}{}
	}
	for _, out := range b.outputs {
		lg.knownTableIDs[out] = struct{}{}
	}
}

// appendRecord marshals, frames and fsyncs one record. Callers must hold lg.mu.
func (lg *flattenLedger) appendRecord(rec flattenLedgerRecord) error {
	if lg.broken {
		return fmt.Errorf("flatten ledger %d is in a failed state; refusing further writes", lg.jobID)
	}
	if lg.fp == nil {
		return fmt.Errorf("flatten ledger %d is not open for writing", lg.jobID)
	}
	if flattenLedgerTestHook != nil {
		// Abort before anything gets written: the ledger stays intact and the
		// caller (compaction batch) rolls back.
		if err := flattenLedgerTestHook(rec); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	frame := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(payload, y.CastagnoliCrcTable))
	frame = append(frame, payload...)

	if _, err := lg.fp.Write(frame); err != nil {
		lg.broken = true
		return err
	}
	if err := flattenSyncFunc(lg.fp); err != nil {
		lg.broken = true
		return err
	}
	return nil
}

// beginJob archives any previously completed ledger, creates a fresh file and
// durably records JobStart. targetLevel is the consolidation sink; fence is the
// next file id captured before the first batch, so table ids >= fence are known
// to have been born while the job was running.
func beginFlattenJob(dir string, targetLevel int, fence uint64) (*flattenLedger, error) {
	// If a completed ledger is sitting in place (job completed but the archive
	// rename failed previously), move it aside before creating a new job.
	if prior, err := readFlattenLedger(dir); err != nil {
		return nil, err
	} else if prior != nil {
		if !prior.complete {
			return nil, fmt.Errorf("cannot begin flatten job: %s is still open", flattenLedgerPath(dir))
		}
		if err := prior.close(); err != nil {
			return nil, err
		}
		if err := archiveFlattenLedger(dir, prior.jobID); err != nil {
			return nil, err
		}
	}

	lg, err := createFlattenLedger(dir)
	if err != nil {
		return nil, err
	}
	lg.mu.Lock()
	defer lg.mu.Unlock()
	lg.jobID = uint64(time.Now().UnixNano())
	lg.targetLevel = targetLevel
	lg.fence = fence
	rec := flattenLedgerRecord{
		Type:      flattenLedgerRecordJobStart,
		JobID:     lg.jobID,
		Target:    targetLevel,
		Fence:     fence,
		CreatedAt: time.Now().UnixNano(),
	}
	if err := lg.appendRecord(rec); err != nil {
		_ = lg.fp.Close()
		return nil, err
	}
	return lg, nil
}

// prepareBatch durably records the intent to merge inputs (each tagged with its
// current level) into outputs at toLevel. It MUST be called before the main
// MANIFEST commits the batch: a failure here aborts the batch with no MANIFEST
// change at all, so the input tables stay live and the freshly built output
// files are discarded by the caller -- the whole batch remains at its last
// usable state.
func (lg *flattenLedger) prepareBatch(fromLevel, toLevel int,
	inputs []ledgerTableRef, outputs []uint64) (uint64, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	seq := uint64(len(lg.batches)) + 1
	rec := flattenLedgerRecord{
		Type:      flattenLedgerRecordBatchPrepare,
		BatchSeq:  seq,
		FromLevel: fromLevel,
		ToLevel:   toLevel,
		Inputs:    inputs,
		Outputs:   outputs,
	}
	if err := lg.appendRecord(rec); err != nil {
		return 0, err
	}
	b := &flattenBatch{
		seq:       seq,
		fromLevel: fromLevel,
		toLevel:   toLevel,
		inputs:    append([]ledgerTableRef(nil), inputs...),
		outputs:   append([]uint64(nil), outputs...),
	}
	lg.batches = append(lg.batches, b)
	lg.prepared[seq] = b
	lg.rememberRefs(b)
	return seq, nil
}

// commitBatch records that the batch's MANIFEST change set is durable and the
// in-memory levels already point at the outputs. A failure here cannot undo the
// committed MANIFEST; the next run reconciles and repairs the ledger
// deterministically. Flatten still aborts, so the failure is surfaced instead
// of silently leaving an open job.
func (lg *flattenLedger) commitBatch(seq uint64) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	b, ok := lg.prepared[seq]
	if !ok {
		// Internal error: every commit must pair with a prepare in this run.
		return fmt.Errorf("flatten ledger: commit for unprepared batch %d", seq)
	}
	if err := lg.appendRecord(flattenLedgerRecord{
		Type:     flattenLedgerRecordBatchCommit,
		BatchSeq: seq,
	}); err != nil {
		return err
	}
	delete(lg.prepared, seq)
	b.committed = true
	return nil
}

// abortBatch marks a prepared batch as aborted during recovery, persisting the
// reason for operators.
func (lg *flattenLedger) abortBatch(seq uint64, reason string) error {
	b, ok := lg.prepared[seq]
	if !ok {
		return nil
	}
	if err := lg.appendRecord(flattenLedgerRecord{
		Type:     flattenLedgerRecordBatchAbort,
		BatchSeq: seq,
		Reason:   reason,
	}); err != nil {
		return err
	}
	delete(lg.prepared, seq)
	b.aborted = true
	return nil
}

// bornAround runs fn (which durably adds the given L0 table to MANIFEST) while
// holding the ledger lock, recording a TableBorn first if the id is new. The
// lock span covers the MANIFEST create so the completion gate cannot observe a
// state in which the table exists in MANIFEST but the ledger hasn't registered
// it. A ledger append failure marks the ledger broken and fails the flush; the
// flush retries like for any other error, and flatten aborts at the next gate.
func (lg *flattenLedger) bornAround(tableID uint64, fn func() error) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if _, ok := lg.knownTableIDs[tableID]; !ok {
		if err := lg.appendRecord(flattenLedgerRecord{
			Type:    flattenLedgerRecordTableBorn,
			TableID: tableID,
		}); err != nil {
			return err
		}
		lg.knownTableIDs[tableID] = struct{}{}
	}
	return fn()
}

// reconcile pairs every prepared-but-unresolved batch with the replayed
// MANIFEST truth and brings the ledger in sync:
//
//   - all outputs live at the destination level and no input left -> the
//     MANIFEST change set committed despite the missing BatchCommit frame:
//     repair with a BatchCommit record;
//   - inputs still live and none of the outputs exist -> the change set never
//     committed (startup removed the orphan files): record BatchAbort, the
//     inputs will simply be picked again;
//   - any mixed state is impossible under atomic MANIFEST change sets, so it is
//     reported as a hard error instead of guessing.
//
// It also backfills TableBorn records for flush tables (id >= fence) that
// landed in MANIFEST in a crash window before they participated in any batch.
func (lg *flattenLedger) reconcile(tables map[uint64]TableManifest) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	lg.resume = true

	// Stable order for deterministic records/logs.
	seqs := make([]uint64, 0, len(lg.prepared))
	for seq := range lg.prepared {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	for _, seq := range seqs {
		b := lg.prepared[seq]
		var outputsLive, inputsLive int
		for _, out := range b.outputs {
			if tm, ok := tables[out]; ok {
				if tm.Level != uint8(b.toLevel) {
					return fmt.Errorf("%w: batch %d output %d is at level %d, expected %d",
						errFlattenLedgerInconsistent, seq, out, tm.Level, b.toLevel)
				}
				outputsLive++
			}
		}
		for _, in := range b.inputs {
			if _, ok := tables[in.ID]; ok {
				inputsLive++
			}
		}
		switch {
		case outputsLive == len(b.outputs) && inputsLive == 0:
			if err := lg.appendRecord(flattenLedgerRecord{
				Type:     flattenLedgerRecordBatchCommit,
				BatchSeq: seq,
			}); err != nil {
				return err
			}
			delete(lg.prepared, seq)
			b.committed = true
		case outputsLive == 0 && inputsLive > 0:
			if err := lg.abortBatch(seq,
				"reconciled: inputs still live in MANIFEST and outputs absent; batch will retry"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: batch %d has %d/%d live outputs and %d live inputs;"+
				" manual inspection required",
				errFlattenLedgerInconsistent, seq, outputsLive, len(b.outputs), inputsLive)
		}
	}

	// Backfill flush-born tables the ledger never got to reference.
	born := make([]uint64, 0)
	for id := range tables {
		if id >= lg.fence {
			if _, ok := lg.knownTableIDs[id]; !ok {
				born = append(born, id)
			}
		}
	}
	sort.Slice(born, func(i, j int) bool { return born[i] < born[j] })
	for _, id := range born {
		if err := lg.appendRecord(flattenLedgerRecord{
			Type:    flattenLedgerRecordTableBorn,
			TableID: id,
		}); err != nil {
			return err
		}
		lg.knownTableIDs[id] = struct{}{}
	}
	return nil
}

// canFinishLocked reports whether the job is allowed to record JobComplete.
// Callers must hold lg.mu (tryFinish does). quiescent is the caller-side
// precondition that no memtable is queued or mid-flush.
func (lg *flattenLedger) canFinishLocked(tables map[uint64]TableManifest, quiescent bool) bool {
	if lg.broken || lg.complete {
		return false
	}
	if !quiescent {
		return false
	}
	// A batch between BatchPrepare and BatchCommit could still be committing;
	// the loop only checks after workers returned, but stay defensive.
	if len(lg.prepared) > 0 {
		return false
	}
	// Every table born during the run (id >= fence) must be known to the
	// ledger. Tables with id < fence were all captured by JobStart's inventory
	// implicitly -- they either got consumed by a batch or still live below the
	// target, in which case the caller would not see a consolidated tree.
	for id := range tables {
		if id >= lg.fence {
			if _, ok := lg.knownTableIDs[id]; !ok {
				return false
			}
		}
	}
	return true
}

// tryFinish appends JobComplete when both the live-tree preconditions (handled
// by the caller: one non-empty level at finalLevel, no pending memtables) and
// the ledger preconditions hold. It performs the final MANIFEST snapshot while
// holding the ledger lock, and the flush path takes the same lock around its
// MANIFEST create, so a table cannot be born between the check and the
// JobComplete append. On success the ledger is closed and archived.
func (lg *flattenLedger) tryFinish(quiescent bool, finalLevel int,
	snapshot func() map[uint64]TableManifest) (bool, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if lg.broken {
		// A previous append/sync failed: spinning here would wedge flatten
		// forever. Fail the run; the ledger on disk is intact up to the last
		// durable record and the next run reconciles.
		return false, errFlattenLedgerBroken
	}
	tables := snapshot()
	if !lg.canFinishLocked(tables, quiescent) {
		return false, nil
	}
	if err := lg.appendRecord(flattenLedgerRecord{
		Type:       flattenLedgerRecordJobComplete,
		FinalLevel: finalLevel,
		CreatedAt:  time.Now().UnixNano(),
	}); err != nil {
		return false, err
	}
	lg.complete = true

	// Archive for audit. Rename happens after close (Windows requirement).
	fp := lg.fp
	dir, jobID := lg.dir, lg.jobID
	if err := fp.Close(); err != nil {
		return true, err
	}
	lg.fp = nil
	if err := archiveFlattenLedger(dir, jobID); err != nil {
		// The job is durably complete in the file content; a failed rename only
		// costs the archive, the next Flatten run moves it aside before starting.
		return true, err
	}
	return true, nil
}

func archiveFlattenLedger(dir string, jobID uint64) error {
	from := flattenLedgerPath(dir)
	to := flattenLedgerDonePath(dir, jobID)
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncDir(dir)
}

func (lg *flattenLedger) close() error {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if lg.fp == nil {
		return nil
	}
	err := lg.fp.Close()
	lg.fp = nil
	return err
}

// FlattenTableStatus describes the migration state of one live table.
type FlattenTableStatus struct {
	// ID is the table's SST file id -- its identity for the lifetime of that
	// physical file.
	ID uint64
	// FromLevel is the level this physical table entered the job at (or the
	// source of its last recorded hop when it is an output of an earlier hop).
	FromLevel int
	// ToLevel is where it currently resides. It equals FromLevel until the
	// table participates in a compaction.
	ToLevel int
	// BatchSeq is the last committed batch that moved this table id; 0 if the
	// table has not moved yet.
	BatchSeq uint64
	// Done is true once the table sits at the job's target level.
	Done bool
}

// FlattenStatus is the operator-facing view of a flatten ledger.
type FlattenStatus struct {
	JobID                uint64
	Resume               bool
	Complete             bool
	TargetLevel          int
	FenceNextFileID      uint64
	Total, Done, Pending int
	Tables               []FlattenTableStatus
}

// status builds FlattenStatus from ledger state and a live MANIFEST snapshot.
// Caller must not hold lg.mu.
func (lg *flattenLedger) status(tables map[uint64]TableManifest) *FlattenStatus {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// lastHop[id] = the latest committed batch that consumed/produced this id.
	type hop struct {
		from, to int
		seq      uint64
	}
	lastHop := make(map[uint64]hop)
	for _, b := range lg.batches {
		if !b.committed {
			continue
		}
		for _, in := range b.inputs {
			lastHop[in.ID] = hop{from: b.fromLevel, to: b.toLevel, seq: b.seq}
		}
		for _, out := range b.outputs {
			lastHop[out] = hop{from: b.fromLevel, to: b.toLevel, seq: b.seq}
		}
	}

	st := &FlattenStatus{
		JobID:           lg.jobID,
		Resume:          lg.resume,
		Complete:        lg.complete,
		TargetLevel:     lg.targetLevel,
		FenceNextFileID: lg.fence,
	}
	ids := make([]uint64, 0, len(tables))
	for id := range tables {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		level := int(tables[id].Level)
		ts := FlattenTableStatus{ID: id, FromLevel: level, ToLevel: level}
		if h, ok := lastHop[id]; ok {
			ts.FromLevel = h.from
			ts.ToLevel = level
			ts.BatchSeq = h.seq
		}
		ts.Done = level == lg.targetLevel
		st.Tables = append(st.Tables, ts)
		st.Total++
		if ts.Done {
			st.Done++
		} else {
			st.Pending++
		}
	}
	return st
}
