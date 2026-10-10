/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

const (
	// RotationFileName is the name of the durable, append-only rotation reconciliation
	// journal. It lives next to KEYREGISTRY and is the single source of truth for
	// which master-key version is in force and how far a rotation batch has progressed.
	RotationFileName = "ROTATION"

	rotationStageDirName = "ROTATION-STAGE"
	keyRegistryBackup    = "KEYREGISTRY.prev"
	stageFileSuffix      = ".key"

	// maxJournalRecordLen caps a single journal record so a torn length field after
	// a crash can be told apart from genuine mid-file corruption.
	maxJournalRecordLen = 64 << 20
	// maxAbortReasonLen bounds the human-readable abort reason stored in the journal.
	maxAbortReasonLen = 512
)

// Record tags used inside the journal.
const (
	recBegin byte = iota + 1
	recPlan
	recKeyDone
	recCommit
	recComplete
	recAbort
)

var (
	// vlogNamePattern matches Badger-owned value log file names (e.g. "000123.vlog").
	vlogNamePattern = regexp.MustCompile(`^\d{6}\.vlog$`)

	// ErrPendingRotationMismatch is returned when the caller-supplied old/new master
	// keys do not match the keys of the rotation batch found unfinished in the journal.
	ErrPendingRotationMismatch = errors.New(
		"ROTATION journal has an unfinished rotation with different old/new keys; " +
			"re-run with the same key pair that started it")
	// ErrReferencedDataKeyMissing is returned when an existing value log file or SSTable
	// references a data key that is absent from the key registry. Rotation must not start
	// from such a state.
	ErrReferencedDataKeyMissing = errors.New(
		"data key referenced by existing value log/SSTable is missing from key registry")
	// ErrVlogHeaderUnreadable is returned when a Badger-owned vlog file does not carry
	// a readable keyID/baseIV header.
	ErrVlogHeaderUnreadable = errors.New("value log file header is unreadable")
	// ErrJournalCorrupt is returned when the rotation journal is damaged somewhere other
	// than its (repairable) tail.
	ErrJournalCorrupt = errors.New("ROTATION journal is corrupted")
	// ErrKeyRotationFailed is wrapped into the returned error whenever a rotation batch
	// was rolled back to the last usable registry version.
	ErrKeyRotationFailed = errors.New("master key rotation failed and was rolled back")
)

// SimulatedCrash is returned by the internal test hook to emulate a hard process
// kill: the engine returns immediately without running rollback, leaving durable
// state exactly as it is on disk so resume can be exercised.
var SimulatedCrash = errors.New("simulated crash")

// RotationOptions configures a replayable master key rotation.
type RotationOptions struct {
	// Dir is the directory holding KEYREGISTRY, MANIFEST and SSTables.
	Dir string
	// ValueDir is the directory holding value log files. Defaults to Dir when empty.
	ValueDir string
	// OldEncryptionKey is the master key currently wrapping the data keys. It may be
	// empty when rotating a plaintext database into an encrypted one.
	OldEncryptionKey []byte
	// NewEncryptionKey is the master key the data keys shall be wrapped with. It may be
	// empty when rotating into plaintext.
	NewEncryptionKey []byte
	// ExternalMagicVersion must match the magic of the application that created the DB.
	ExternalMagicVersion uint16
}

// RotationReport summarizes what a reconciliation pass did.
type RotationReport struct {
	// RotationID is the identifier of the rotation batch that was resumed or started.
	RotationID uint64
	// Resumed is true if an unfinished batch was found in the journal and continued.
	Resumed bool
	// DataKeysRewrapped counts data keys actually (re)wrapped under the new master key
	// during this pass. Keys already wrapped in a previous pass and verified are not counted.
	DataKeysRewrapped int
	// ReferencedDataKeys are data keys still referenced by existing vlog/SSTable files;
	// these cannot be retired.
	ReferencedDataKeys []uint64
	// RolledBack is true when the batch was rolled back to the last usable version.
	RolledBack bool
	// Completed is true when the new master key version is now fully in force.
	Completed bool
}

// payloadBuilder is a minimal deterministic record encoder.
type payloadBuilder struct {
	b []byte
}

func (p *payloadBuilder) writeByte(v byte) { p.b = append(p.b, v) }
func (p *payloadBuilder) writeUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	p.b = append(p.b, buf[:n]...)
}
func (p *payloadBuilder) writeFixed(v []byte) { p.b = append(p.b, v...) }

// payloadReader is the matching decoder.
type payloadReader struct {
	b   []byte
	err error
}

func (r *payloadReader) readByte() byte {
	if len(r.b) < 1 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}
func (r *payloadReader) readUvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	r.b = r.b[n:]
	return v
}
func (r *payloadReader) readFixed(n int) []byte {
	if len(r.b) < n {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
func (r *payloadReader) end() error {
	if r.err != nil {
		return r.err
	}
	if len(r.b) != 0 {
		return fmt.Errorf("trailing %d bytes in journal record", len(r.b))
	}
	return nil
}

// rotationPlan is the in-memory projection of one rotation batch.
type rotationPlan struct {
	id           uint64
	createdAt    int64
	oldFP        []byte // sha256 of old master key
	newFP        []byte // sha256 of new master key
	keys         []uint64
	referenced   []uint64
	done         map[uint64]bool
	committed    bool
	registryHash []byte // sha256 of the committed registry bytes
	complete     bool
	aborted      bool
	reason       string
}

func (p *rotationPlan) pending() bool { return p != nil && !p.complete && !p.aborted }

// rotationState is the projection of the whole journal.
type rotationState struct {
	lastID      uint64
	pending     *rotationPlan
	tailDamaged bool
}

// rotationJournal is the durable append-only ROTATION file.
type rotationJournal struct {
	fp   *os.File
	path string
	mu   sync.Mutex

	state rotationState
}

// encodeFrame wraps a payload with its length and CRC32C.
func encodeFrame(payload []byte) []byte {
	var frame [8]byte
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(payload, y.CastagnoliCrcTable))
	out := make([]byte, 0, 8+len(payload))
	out = append(out, frame[:]...)
	out = append(out, payload...)
	return out
}

// openRotationJournal opens the journal and replays it, repairing a torn tail left
// by a crash. The file is created lazily on first append, so a database that never
// rotated carries no ROTATION file.
func openRotationJournal(dir string) (*rotationJournal, error) {
	path := filepath.Join(dir, RotationFileName)
	fp, err := os.OpenFile(path, os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		return &rotationJournal{path: path}, nil
	}
	if err != nil {
		return nil, y.Wrapf(err, "while opening rotation journal")
	}
	j := &rotationJournal{fp: fp, path: path}
	if err := j.replay(); err != nil {
		fp.Close()
		return nil, err
	}
	return j, nil
}

// replay reads every framed record. A record that fails to parse at the end of the
// file is treated as a torn write and truncated; failures elsewhere are fatal.
func (j *rotationJournal) replay() error {
	stat, err := j.fp.Stat()
	if err != nil {
		return y.Wrapf(err, "while stat-ing rotation journal")
	}
	fileSize := stat.Size()
	var offset int64
	j.fp.Seek(0, io.SeekStart)

	for offset < fileSize {
		var hdr [8]byte
		n, err := io.ReadFull(j.fp, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Torn length/header at the tail.
			if n > 0 {
				if err := j.truncateTail(offset); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return y.Wrapf(err, "while reading rotation journal")
		}
		length := int64(binary.BigEndian.Uint32(hdr[0:4]))
		end := offset + 8 + length
		if length > maxJournalRecordLen || end > fileSize {
			// Length field itself was torn. Only repairable when nothing valid follows.
			return j.truncateTail(offset)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(j.fp, payload); err != nil {
			// Short payload at the tail.
			return j.truncateTail(offset)
		}
		// Frame CRC lives in the header (hdr[4:8]), right after the length.
		midFile := end != fileSize
		if crc32.Checksum(payload, y.CastagnoliCrcTable) !=
			binary.BigEndian.Uint32(hdr[4:8]) {
			if midFile {
				return y.Wrapf(ErrJournalCorrupt,
					"checksum mismatch at offset %d", offset)
			}
			return j.truncateTail(offset)
		}
		if err := j.apply(payload); err != nil {
			return y.Wrapf(err, "at journal offset %d", offset)
		}
		offset = end
	}
	return nil
}

func (j *rotationJournal) truncateTail(offset int64) error {
	if err := j.fp.Truncate(offset); err != nil {
		return y.Wrapf(err, "while truncating torn rotation journal tail")
	}
	j.state.tailDamaged = true
	return nil
}

// apply folds one decoded record into the journal state.
func (j *rotationJournal) apply(payload []byte) error {
	r := &payloadReader{b: payload}
	typ := r.readByte()
	id := r.readUvarint()

	switch typ {
	case recBegin:
		p := &rotationPlan{
			id:        id,
			createdAt: int64(r.readUvarint()),
			oldFP:     r.readFixed(sha256.Size),
			newFP:     r.readFixed(sha256.Size),
			done:      make(map[uint64]bool),
		}
		if err := r.end(); err != nil {
			return err
		}
		j.state.pending = p
		if id > j.state.lastID {
			j.state.lastID = id
		}
	case recPlan:
		p := j.state.pending
		if p == nil || p.id != id {
			return fmt.Errorf("plan record without matching begin %d", id)
		}
		p.keys = readIDList(r)
		p.referenced = readIDList(r)
		if err := r.end(); err != nil {
			return err
		}
	case recKeyDone:
		p := j.state.pending
		if p == nil || p.id != id {
			return fmt.Errorf("key-done record without matching begin %d", id)
		}
		keyID := r.readUvarint()
		if err := r.end(); err != nil {
			return err
		}
		p.done[keyID] = true
	case recCommit:
		p := j.state.pending
		if p == nil || p.id != id {
			return fmt.Errorf("commit record without matching begin %d", id)
		}
		p.registryHash = r.readFixed(sha256.Size)
		if err := r.end(); err != nil {
			return err
		}
		p.committed = true
	case recComplete:
		p := j.state.pending
		if p == nil || p.id != id {
			return fmt.Errorf("complete record without matching begin %d", id)
		}
		if err := r.end(); err != nil {
			return err
		}
		p.complete = true
		j.state.pending = nil
	case recAbort:
		p := j.state.pending
		if p == nil || p.id != id {
			return fmt.Errorf("abort record without matching begin %d", id)
		}
		reasonLen := r.readUvarint()
		reason := r.readFixed(int(reasonLen))
		if err := r.end(); err != nil {
			return err
		}
		p.aborted = true
		p.reason = string(reason)
		j.state.pending = nil
	default:
		return fmt.Errorf("unknown rotation record type %d", typ)
	}
	return nil
}

func readIDList(r *payloadReader) []uint64 {
	count := r.readUvarint()
	ids := make([]uint64, 0, count)
	for i := uint64(0); i < count; i++ {
		ids = append(ids, r.readUvarint())
	}
	return ids
}

// appendRecord durably appends one framed record, creating the journal on first use.
func (j *rotationJournal) appendRecord(payload []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fp == nil {
		fp, err := os.OpenFile(j.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return y.Wrapf(err, "while creating rotation journal")
		}
		j.fp = fp
	}
	if _, err := j.fp.Write(encodeFrame(payload)); err != nil {
		return y.Wrapf(err, "while writing rotation journal")
	}
	return j.fp.Sync()
}

func (j *rotationJournal) close() error {
	if j.fp == nil {
		return nil
	}
	return j.fp.Close()
}

// --- record builders ---------------------------------------------------------

func beginRecord(p *rotationPlan) []byte {
	b := &payloadBuilder{}
	b.writeByte(recBegin)
	b.writeUvarint(p.id)
	b.writeUvarint(uint64(p.createdAt))
	b.writeFixed(p.oldFP)
	b.writeFixed(p.newFP)
	return b.b
}

func planRecord(id uint64, keys, referenced []uint64) []byte {
	b := &payloadBuilder{}
	b.writeByte(recPlan)
	b.writeUvarint(id)
	writeIDList(b, keys)
	writeIDList(b, referenced)
	return b.b
}

func keyDoneRecord(id, keyID uint64) []byte {
	b := &payloadBuilder{}
	b.writeByte(recKeyDone)
	b.writeUvarint(id)
	b.writeUvarint(keyID)
	return b.b
}

func commitRecord(id uint64, hash []byte) []byte {
	b := &payloadBuilder{}
	b.writeByte(recCommit)
	b.writeUvarint(id)
	b.writeFixed(hash)
	return b.b
}

func completeRecord(id uint64) []byte {
	b := &payloadBuilder{}
	b.writeByte(recComplete)
	b.writeUvarint(id)
	return b.b
}

func abortRecord(id uint64, reason string) []byte {
	if len(reason) > maxAbortReasonLen {
		reason = reason[:maxAbortReasonLen]
	}
	b := &payloadBuilder{}
	b.writeByte(recAbort)
	b.writeUvarint(id)
	b.writeUvarint(uint64(len(reason)))
	b.writeFixed([]byte(reason))
	return b.b
}

func writeIDList(b *payloadBuilder, ids []uint64) {
	b.writeUvarint(uint64(len(ids)))
	for _, id := range ids {
		b.writeUvarint(id)
	}
}

// --- test hook ---------------------------------------------------------------

type rotationHookPoint int

const (
	hookStagePlaced rotationHookPoint = iota
	hookRewriteWritten
	hookAfterSwap
	hookAfterCommit
)

// rotationHookInfo carries context the hook may inspect or manipulate.
type rotationHookInfo struct {
	KeyID       uint64
	StageDir    string
	RewritePath string
}

// rotationHook, when non-nil (tests only), is invoked at crash/failure checkpoints.
// Returning SimulatedCrash emulates a hard kill (no rollback). Any other error
// triggers full batch rollback.
var rotationHook func(point rotationHookPoint, info *rotationHookInfo) error

func invokeHook(point rotationHookPoint, info *rotationHookInfo) error {
	if rotationHook == nil {
		return nil
	}
	return rotationHook(point, info)
}

// --- engine ------------------------------------------------------------------

// rotator drives one reconciliation pass.
type rotator struct {
	opt   RotationOptions
	j     *rotationJournal
	vdir  string
	plan  *rotationPlan
	kr    *KeyRegistry
	stage string

	report *RotationReport
}

// ReconcileKeyRotation runs a replayable master key rotation. If the journal shows
// an unfinished batch whose keys match, it is resumed; otherwise a new batch starts.
// Any failure to persist progress or to validate the registry rolls the whole batch
// back to the last usable version.
func ReconcileKeyRotation(opt RotationOptions) (*RotationReport, error) {
	if err := validateKey(opt.OldEncryptionKey); err != nil {
		return nil, y.Wrapf(err, "invalid old master key")
	}
	if err := validateKey(opt.NewEncryptionKey); err != nil {
		return nil, y.Wrapf(err, "invalid new master key")
	}
	if opt.Dir == "" {
		return nil, errors.New("rotation directory not specified")
	}
	vdir := opt.ValueDir
	if vdir == "" {
		vdir = opt.Dir
	}

	j, err := openRotationJournal(opt.Dir)
	if err != nil {
		return nil, err
	}
	defer j.close()

	e := &rotator{
		opt:    opt,
		j:      j,
		vdir:   vdir,
		stage:  filepath.Join(opt.Dir, rotationStageDirName),
		report: &RotationReport{},
	}

	if p := j.state.pending; p.pending() {
		if !bytes.Equal(p.oldFP, keyFingerprint(opt.OldEncryptionKey)) ||
			!bytes.Equal(p.newFP, keyFingerprint(opt.NewEncryptionKey)) {
			return nil, ErrPendingRotationMismatch
		}
		e.plan = p
		e.report.RotationID = p.id
		e.report.Resumed = true
		e.report.ReferencedDataKeys = append([]uint64(nil), p.referenced...)
	} else {
		if err := e.begin(); err != nil {
			return nil, err
		}
	}

	if err := e.runToFinish(); err != nil {
		return e.report, err
	}
	return e.report, nil
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return nil
	}
	switch len(key) {
	case 16, 24, 32:
		return nil
	default:
		return ErrInvalidEncryptionKey
	}
}

func keyFingerprint(key []byte) []byte {
	sum := sha256.Sum256(key)
	return sum[:]
}

// begin prepares a brand new rotation batch: it validates the current registry and
// live references, snapshots the registry, then durably records BEGIN + PLAN.
func (e *rotator) begin() error {
	regPath := filepath.Join(e.opt.Dir, KeyRegistryFileName)
	kr, err := probeRegistryAt(regPath, e.opt.OldEncryptionKey)
	if err != nil {
		return y.Wrapf(err, "while reading current key registry")
	}
	e.kr = kr

	referenced, err := e.scanReferences()
	if err != nil {
		return err
	}
	if missing := missingKeys(kr, referenced); len(missing) > 0 {
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		return fmt.Errorf("%w: key ids %v are referenced on disk but absent from registry",
			ErrReferencedDataKeyMissing, missing)
	}

	keys := make([]uint64, 0, len(kr.dataKeys))
	for id := range kr.dataKeys {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	sort.Slice(referenced, func(i, j int) bool { return referenced[i] < referenced[j] })

	// Snapshot the last known-good registry before touching anything.
	registryBytes, err := os.ReadFile(regPath)
	if err != nil {
		return y.Wrapf(err, "while snapshotting key registry")
	}
	backupPath := filepath.Join(e.opt.Dir, keyRegistryBackup)
	if err := durableWriteFile(backupPath, registryBytes, e.opt.Dir); err != nil {
		return err
	}

	p := &rotationPlan{
		id:         e.j.state.lastID + 1,
		createdAt:  time.Now().Unix(),
		oldFP:      keyFingerprint(e.opt.OldEncryptionKey),
		newFP:      keyFingerprint(e.opt.NewEncryptionKey),
		keys:       keys,
		referenced: referenced,
		done:       make(map[uint64]bool),
	}
	// e.plan must be set before the first journal append: a failed append triggers
	// rollback, which needs a plan to write the ABORT record.
	e.plan = p
	e.report.RotationID = p.id
	e.report.ReferencedDataKeys = append([]uint64(nil), referenced...)

	if err := e.j.appendRecord(beginRecord(p)); err != nil {
		e.rollback(err)
		return err
	}
	if err := e.j.appendRecord(planRecord(p.id, keys, referenced)); err != nil {
		e.rollback(err)
		return err
	}
	return nil
}

// runToFinish is shared by fresh and resumed batches.
func (e *rotator) runToFinish() error {
	p := e.plan

	// Figure out which key currently wraps the on-disk registry.
	regPath := filepath.Join(e.opt.Dir, KeyRegistryFileName)
	newKR, newErr := probeRegistryAt(regPath, e.opt.NewEncryptionKey)
	oldKR, oldErr := probeRegistryAt(regPath, e.opt.OldEncryptionKey)

	switch {
	case newErr == nil && sameKeySet(newKR, p.keys):
		// Swap already happened (crash between rename and COMMIT/COMPLETE).
		return e.commitAndFinish()
	case oldErr == nil:
		// Pre-swap state: reconcile staging then perform the swap.
		e.kr = oldKR
		// Crash window: BEGIN persisted but PLAN did not. Rebuild the plan from
		// the current registry rather than rolling back.
		if len(p.keys) == 0 {
			p.keys = sortedRegistryKeys(oldKR)
			refs, err := e.scanReferences()
			if err != nil {
				return e.rollback(err)
			}
			p.referenced = refs
			if err := e.j.appendRecord(planRecord(p.id, p.keys, refs)); err != nil {
				return e.rollback(err)
			}
		}
		if err := e.reconcileStage(); err != nil {
			if errors.Is(err, SimulatedCrash) {
				return err
			}
			return e.rollback(err)
		}
		if err := e.stageRemaining(); err != nil {
			if errors.Is(err, SimulatedCrash) {
				return err
			}
			return e.rollback(err)
		}
		if _, err := e.swap(); err != nil {
			if errors.Is(err, SimulatedCrash) {
				return err
			}
			return e.rollback(err)
		}
		return e.commitAndFinish()
	default:
		// Neither key opens the registry: disk copy is damaged.
		return e.rollback(fmt.Errorf("key registry on disk opens with neither key: "+
			"(new-key err: %v; old-key err: %v)", newErr, oldErr))
	}
}

// reconcileStage scans the staging directory and aligns journal KEY_DONE markers
// with what is durably on disk. Staged files are independently verified against
// the plaintext data key, so a half-written or corrupt stage file is discarded.
func (e *rotator) reconcileStage() error {
	if err := os.MkdirAll(e.stage, 0700); err != nil {
		return y.Wrapf(err, "while creating rotation stage")
	}
	entries, err := os.ReadDir(e.stage)
	if err != nil {
		return y.Wrapf(err, "while reading rotation stage")
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		keyID, ok := parseStageName(name)
		if !ok {
			// Unknown file inside our own stage directory: leave it alone.
			continue
		}
		dk, err := e.kr.DataKey(keyID)
		if err != nil || dk == nil {
			// Stale stage entry for a key no longer in the plan: remove it.
			_ = os.Remove(filepath.Join(e.stage, name))
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.stage, name))
		if err != nil || verifyStageFile(data, dk, e.opt.NewEncryptionKey) != nil {
			_ = os.Remove(filepath.Join(e.stage, name))
			continue
		}
		if !e.plan.done[keyID] {
			if err := e.j.appendRecord(keyDoneRecord(e.plan.id, keyID)); err != nil {
				return err
			}
			e.plan.done[keyID] = true
		}
	}
	return nil
}

// stageRemaining wraps every data key that has no verified staged copy yet.
func (e *rotator) stageRemaining() error {
	for _, id := range e.plan.keys {
		if e.plan.done[id] {
			continue
		}
		dk, err := e.kr.DataKey(id)
		if err != nil {
			return err
		}
		if err := e.placeStageFile(dk); err != nil {
			return err
		}
		if err := invokeHook(hookStagePlaced, &rotationHookInfo{
			KeyID: id, StageDir: e.stage,
		}); err != nil {
			return err
		}
		if err := e.j.appendRecord(keyDoneRecord(e.plan.id, id)); err != nil {
			return err
		}
		e.plan.done[id] = true
		e.report.DataKeysRewrapped++
	}
	return nil
}

// placeStageFile wraps one data key under the new master key and durably stores it.
func (e *rotator) placeStageFile(dk *pb.DataKey) error {
	iv, err := y.GenerateIV()
	if err != nil {
		return err
	}
	wrapped := dk.Data
	if len(e.opt.NewEncryptionKey) > 0 {
		wrapped, err = y.XORBlockAllocate(dk.Data, e.opt.NewEncryptionKey, iv)
		if err != nil {
			return err
		}
	}
	b := &payloadBuilder{}
	b.writeUvarint(dk.KeyId)
	b.writeFixed(iv)
	b.writeFixed(wrapped)
	path := filepath.Join(e.stage, stageFileName(dk.KeyId))
	return durableWriteFile(path, encodeFrame(b.b), e.stage)
}

// verifyStageFile checks a framed staged file: frame CRC, key id, unwrap with the
// new master key, and equality with the expected plaintext data key.
func verifyStageFile(record []byte, expected *pb.DataKey, newKey []byte) error {
	payload, err := parseFrame(record)
	if err != nil {
		return err
	}
	r := &payloadReader{b: payload}
	id := r.readUvarint()
	iv := r.readFixed(aesBlockSize)
	wrapped := r.readFixed(len(r.b))
	if err := r.end(); err != nil {
		return err
	}
	if id != expected.KeyId {
		return fmt.Errorf("stage file key id %d != expected %d", id, expected.KeyId)
	}
	plain := wrapped
	if len(newKey) > 0 {
		plain, err = y.XORBlockAllocate(wrapped, newKey, iv)
		if err != nil {
			return err
		}
	}
	if !bytes.Equal(plain, expected.Data) {
		return errors.New("staged data key does not match registry")
	}
	return nil
}

// swap assembles the complete registry under the new master key, writes it to the
// rewrite file, validates it independently, and atomically renames it into place.
func (e *rotator) swap() (*KeyRegistry, error) {
	data, err := marshalKeyRegistry(e.kr, e.opt.NewEncryptionKey)
	if err != nil {
		return nil, err
	}
	rewritePath := filepath.Join(e.opt.Dir, KeyRegistryRewriteFileName)
	if err := durableWriteFile(rewritePath, data, e.opt.Dir); err != nil {
		return nil, err
	}
	if err := invokeHook(hookRewriteWritten, &rotationHookInfo{
		RewritePath: rewritePath,
	}); err != nil {
		return nil, err
	}
	// Independently read the rewrite back with the new key.
	newKR, err := probeRegistryAt(rewritePath, e.opt.NewEncryptionKey)
	if err != nil {
		return nil, y.Wrapf(err, "while validating rewritten key registry")
	}
	if !sameKeySet(newKR, e.plan.keys) {
		return nil, fmt.Errorf("rewritten registry key set does not match plan")
	}
	if missing := missingKeys(newKR, e.plan.referenced); len(missing) > 0 {
		return nil, fmt.Errorf("%w: referenced key ids %v would be dropped by rotation",
			ErrReferencedDataKeyMissing, missing)
	}
	// Atomically replace the live registry.
	if err := os.Rename(rewritePath, filepath.Join(e.opt.Dir, KeyRegistryFileName)); err != nil {
		return nil, y.Wrapf(err, "while renaming rewritten key registry")
	}
	if err := syncDir(e.opt.Dir); err != nil {
		return nil, err
	}
	if err := invokeHook(hookAfterSwap, &rotationHookInfo{}); err != nil {
		return nil, err
	}
	return newKR, nil
}

// commitAndFinish records COMMIT (with the live registry hash), then COMPLETE, and
// removes all rotation-only artifacts.
func (e *rotator) commitAndFinish() error {
	if !e.plan.committed {
		// Hash the bytes actually on disk, not a re-marshalled copy (marshalling
		// generates a fresh random IV each time).
		liveBytes, err := os.ReadFile(filepath.Join(e.opt.Dir, KeyRegistryFileName))
		if err != nil {
			return e.rollback(err)
		}
		hash := sha256.Sum256(liveBytes)
		if err := e.j.appendRecord(commitRecord(e.plan.id, hash[:])); err != nil {
			return e.rollback(err)
		}
		e.plan.committed = true
	}
	if err := invokeHook(hookAfterCommit, &rotationHookInfo{}); err != nil {
		if errors.Is(err, SimulatedCrash) {
			return err
		}
		return e.rollback(err)
	}

	// Final sanity read of the live registry with the new key.
	live, err := probeRegistryAt(filepath.Join(e.opt.Dir, KeyRegistryFileName),
		e.opt.NewEncryptionKey)
	if err != nil {
		return e.rollback(err)
	}
	if !sameKeySet(live, e.plan.keys) {
		return e.rollback(errors.New("live registry does not match plan"))
	}
	if err := e.j.appendRecord(completeRecord(e.plan.id)); err != nil {
		return e.rollback(err)
	}
	e.cleanup()
	e.report.Completed = true
	return nil
}

// rollback restores the last usable registry version, clears the stage and records
// ABORT. It must be idempotent and never return a panic.
func (e *rotator) rollback(cause error) error {
	backupPath := filepath.Join(e.opt.Dir, keyRegistryBackup)
	if data, err := os.ReadFile(backupPath); err == nil {
		rewritePath := filepath.Join(e.opt.Dir, KeyRegistryRewriteFileName)
		if err := durableWriteFile(rewritePath, data, e.opt.Dir); err == nil {
			if _, perr := probeRegistryAt(rewritePath, e.opt.OldEncryptionKey); perr == nil {
				if rerr := os.Rename(rewritePath,
					filepath.Join(e.opt.Dir, KeyRegistryFileName)); rerr == nil {
					_ = syncDir(e.opt.Dir)
				}
			}
		}
	}
	// Stage artifacts are ours; remove them.
	if entries, err := os.ReadDir(e.stage); err == nil {
		for _, ent := range entries {
			if _, ok := parseStageName(ent.Name()); ok && !ent.IsDir() {
				_ = os.Remove(filepath.Join(e.stage, ent.Name()))
			}
		}
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	_ = e.j.appendRecord(abortRecord(e.plan.id, reason))
	e.report.RolledBack = true
	return fmt.Errorf("%w: %v", ErrKeyRotationFailed, cause)
}

// cleanup removes rotation-only artifacts after a successful rotation.
func (e *rotator) cleanup() {
	_ = os.RemoveAll(e.stage)
	_ = os.Remove(filepath.Join(e.opt.Dir, keyRegistryBackup))
	_ = os.Remove(filepath.Join(e.opt.Dir, KeyRegistryRewriteFileName))
	_ = syncDir(e.opt.Dir)
	if e.vdir != e.opt.Dir {
		_ = syncDir(e.vdir)
	}
}

// --- references and file ownership ------------------------------------------

// scanReferences computes the set of data key ids still referenced by existing
// on-disk data: SSTables via MANIFEST, value logs via their file headers.
func (e *rotator) scanReferences() ([]uint64, error) {
	refs := make(map[uint64]struct{})

	// SSTables: only those recorded in MANIFEST are Badger-owned.
	manifestPath := filepath.Join(e.opt.Dir, ManifestFilename)
	if fp, err := os.Open(manifestPath); err == nil {
		manifest, _, err := ReplayManifestFile(fp, e.opt.ExternalMagicVersion,
			DefaultOptions(e.opt.Dir))
		fp.Close()
		if err != nil {
			return nil, y.Wrapf(err, "while reading MANIFEST")
		}
		for _, tm := range manifest.Tables {
			if tm.KeyID != 0 {
				refs[tm.KeyID] = struct{}{}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, y.Wrapf(err, "while opening MANIFEST")
	}

	// Value logs: read the keyID from each Badger-owned file header.
	entries, err := os.ReadDir(e.vdir)
	if err != nil {
		return nil, y.Wrapf(err, "while listing value log directory")
	}
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !vlogNamePattern.MatchString(name) {
			continue
		}
		keyID, err := readVlogKeyID(filepath.Join(e.vdir, name))
		if err != nil {
			return nil, fmt.Errorf("vlog %s: %w", name, err)
		}
		if keyID != 0 {
			refs[keyID] = struct{}{}
		}
	}

	out := make([]uint64, 0, len(refs))
	for id := range refs {
		out = append(out, id)
	}
	return out, nil
}

// readVlogKeyID reads the 8-byte keyID prefix of a vlog file.
func readVlogKeyID(path string) (uint64, error) {
	fp, err := os.Open(path)
	if err != nil {
		return 0, y.Wrapf(err, "while opening vlog")
	}
	defer fp.Close()
	fi, err := fp.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() < vlogHeaderSize {
		return 0, ErrVlogHeaderUnreadable
	}
	var buf [8]byte
	if _, err := io.ReadFull(fp, buf[:]); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrVlogHeaderUnreadable, err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func missingKeys(kr *KeyRegistry, ids []uint64) []uint64 {
	var missing []uint64
	for _, id := range ids {
		if _, ok := kr.dataKeys[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func sortedRegistryKeys(kr *KeyRegistry) []uint64 {
	ids := make([]uint64, 0, len(kr.dataKeys))
	for id := range kr.dataKeys {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func sameKeySet(kr *KeyRegistry, ids []uint64) bool {
	if len(kr.dataKeys) != len(ids) {
		return false
	}
	for _, id := range ids {
		if _, ok := kr.dataKeys[id]; !ok {
			return false
		}
	}
	return true
}

// --- small file helpers ------------------------------------------------------

const aesBlockSize = 16

func stageFileName(id uint64) string {
	return fmt.Sprintf("%06d%s", id, stageFileSuffix)
}

func parseStageName(name string) (uint64, bool) {
	if len(name) != 6+len(stageFileSuffix) || name[6:] != stageFileSuffix {
		return 0, false
	}
	var id uint64
	for i := 0; i < 6; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		id = id*10 + uint64(c-'0')
	}
	return id, true
}

// parseFrame validates a length/CRC frame and returns its payload.
// Layout: [length(4)][crc32c(4)][payload].
func parseFrame(record []byte) ([]byte, error) {
	if len(record) < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	length := int(binary.BigEndian.Uint32(record[0:4]))
	if 8+length != len(record) {
		return nil, errors.New("frame length mismatch")
	}
	payload := record[8 : 8+length]
	crc := binary.BigEndian.Uint32(record[4:8])
	if crc32.Checksum(payload, y.CastagnoliCrcTable) != crc {
		return nil, y.ErrChecksumMismatch
	}
	return payload, nil
}

// probeRegistryAt opens a registry file at the given path with the given key.
func probeRegistryAt(path string, key []byte) (*KeyRegistry, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return readKeyRegistry(fp, KeyRegistryOptions{EncryptionKey: key})
}

// durableWriteFile writes data to path via a synced temp file, atomic rename and
// directory fsync.
func durableWriteFile(path string, data []byte, dir string) error {
	tmpPath := path + ".tmp"
	fp, err := y.OpenTruncFile(tmpPath, true)
	if err != nil {
		return y.Wrapf(err, "while opening %s", tmpPath)
	}
	if _, err := fp.Write(data); err != nil {
		fp.Close()
		return y.Wrapf(err, "while writing %s", tmpPath)
	}
	// In Windows the file must be closed before rename; Close also flushes.
	if err := fp.Close(); err != nil {
		return y.Wrapf(err, "while closing %s", tmpPath)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return y.Wrapf(err, "while renaming to %s", path)
	}
	return syncDir(dir)
}
