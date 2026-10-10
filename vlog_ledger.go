/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4/y"
)

// VLOG-LEDGER is a durable, append-only ledger that records, per value-log
// file identity:
//
//   - whether the file is owned by this DB (created/adopted by it) or is a
//     foreign file dropped into the value-log directory by somebody else;
//   - the outcome of value-log GC against the file (running / failed /
//     succeeded), so a file that has already been collected is never
//     collected a second time and a file whose GC was interrupted can be
//     retried until the run converges.
//
// The on-disk layout is a 4-byte magic followed by checksummed frames:
//
//	+-------------------------------------------------------------+
//	| seq(8) | type(1) | flags(1) | payloadLen(4) | payload | crc(4) |
//	+-------------------------------------------------------------+
//
// where crc is a Castagnoli CRC-32 over everything between the frame length
// and the crc itself. Frames are only ever appended and fsynced. A torn or
// corrupt tail (crash/power loss mid-write) can only occur at the end; on
// open it is detected and truncated, and replay resumes from the last valid
// frame. Replaying the same frames always produces the same in-memory state,
// so interrupted runs converge to the same result.
//
// When the log grows past ledgerCompactSize it is compacted atomically: a
// snapshot of the current state is written to a temp file, fsynced and
// renamed over the old one.
const (
	ledgerFname    = "VLOG-LEDGER"
	ledgerMagic    = "VLG1"
	ledgerFrameHdr = 8 + 1 + 1 + 4 // seq | type | flags | payloadLen
	ledgerCRCLen   = 4

	ledgerCompactSize    = 2 << 20 // compact once the log exceeds 2 MiB
	ledgerCompactRecords = 2048    // or once this many frames accumulated
	ledgerMaxErrMsg      = 512
)

// Frame types. Order matters only within a single file identity; replay is a
// pure fold over frames.
const (
	recBootstrap byte = iota + 1
	recCreated
	recAdopted
	recGCStart
	recGCFailed
	recDeleted
	recForeign
)

// File origins recorded at the moment a file becomes managed.
const (
	originCreated uint8 = iota + 1 // created by this DB via valueLog.createVlogFile
	originLegacy                   // adopted from a pre-ledger directory at first boot
	originOrphan                   // adopted as an empty next-fid file left by a crash around create
)

// Reasons a present-on-disk file is classified as foreign.
const (
	foreignUnregistered uint8 = iota + 1 // appeared after bootstrap and was never created by us
	foreignBadName                       // *.vlog whose name is not a numeric file id
	foreignInvalidVlog                   // failed the valid-Badger-vlog probe during adoption
	foreignReappeared                    // sits at a fid we have already deleted
)

// Reasons a managed file reached the deleted state. Informational only.
const (
	delByGC uint8 = iota + 1
	delEmpty
	delMissing
	delDropAll
)

type ledgerState uint8

const (
	fsAlive     ledgerState = iota // managed and expected on disk
	fsGCRunning                    // a GC rewrite was started; interrupted runs fold back to alive
	fsDeleted                      // terminal tombstone; the fid is never reused
)

func (s ledgerState) String() string {
	switch s {
	case fsAlive:
		return "alive"
	case fsGCRunning:
		return "gc_running"
	case fsDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

type ledgerGCResult uint8

const (
	gcNone ledgerGCResult = iota
	gcRunning
	gcFailed
	gcSucceeded
)

func (r ledgerGCResult) String() string {
	switch r {
	case gcRunning:
		return "running"
	case gcFailed:
		return "failed"
	case gcSucceeded:
		return "succeeded"
	default:
		return "none"
	}
}

// ledgerFile is the folded state of one managed value-log file.
type ledgerFile struct {
	fid       uint32
	state     ledgerState
	origin    uint8
	attempts  uint32
	result    ledgerGCResult
	lastError string
	delReason uint8
}

type ledgerForeignFile struct {
	name   string
	size   int64
	reason uint8
}

// gcLedger is the durable value-log file ledger. Zero value is not usable;
// construct with openVlogLedger.
type gcLedger struct {
	dirPath string
	opt     Options

	mu sync.Mutex

	// f is nil until the first append on a not-yet-created ledger, and nil
	// for a read-only DB opened before any ledger was ever written.
	f        *os.File
	writable bool
	off      int64 // one past the last valid byte
	seq      uint64

	bootstrapped bool
	managed      map[uint32]*ledgerFile
	foreign      map[string]*ledgerForeignFile
	// maxFid is the largest fid ever managed, deleted tombstones included.
	// createVlogFile allocates strictly above it, so fids are never reused.
	maxFid uint32

	records int // frames appended since the last compaction
}

func ledgerPath(opt Options) string {
	return filepath.Join(opt.ValueDir, ledgerFname)
}

// openVlogLedger opens (or prepares to create lazily) the durable ledger.
//
// In read-only mode a missing ledger yields an ephemeral, write-less ledger:
// existing files are classified in memory and nothing on disk is touched.
func openVlogLedger(opt Options) (*gcLedger, error) {
	l := &gcLedger{
		dirPath:  opt.ValueDir,
		opt:      opt,
		writable: !opt.ReadOnly,
		managed:  make(map[uint32]*ledgerFile),
		foreign:  make(map[string]*ledgerForeignFile),
	}

	path := ledgerPath(opt)
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Fresh DB: the file is created lazily by the first append
			// (bootstrap/adopt/create/foreign decision).
			return l, nil
		}
		return nil, y.Wrapf(err, "while statting %s", ledgerFname)
	}

	flags := os.O_RDWR
	if opt.ReadOnly {
		flags = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, y.Wrapf(err, "while opening %s", ledgerFname)
	}
	l.f = f
	l.off = fi.Size()

	if fi.Size() == 0 {
		if opt.ReadOnly {
			// An empty ledger in RO mode carries no decisions; behave
			// ephemerally and close the useless descriptor.
			_ = f.Close()
			l.f = nil
			return l, nil
		}
		// Leftover empty file from a crash at creation. Reinitialize it.
		if _, err := f.WriteAt([]byte(ledgerMagic), 0); err != nil {
			return nil, y.Wrapf(err, "while initializing %s", ledgerFname)
		}
		if err := f.Sync(); err != nil {
			return nil, y.Wrapf(err, "while syncing %s", ledgerFname)
		}
		if err := syncDir(opt.ValueDir); err != nil {
			return nil, y.Wrapf(err, "while syncing value dir")
		}
		l.off = int64(len(ledgerMagic))
		return l, nil
	}

	data := make([]byte, fi.Size())
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, y.Wrapf(err, "while reading %s", ledgerFname)
	}
	valid, err := l.replay(data)
	if err != nil {
		return nil, err
	}
	if l.writable && valid < len(data) {
		// Drop the torn/corrupt tail so future appends start at a boundary.
		if err := f.Truncate(int64(valid)); err != nil {
			return nil, y.Wrapf(err, "while truncating torn %s", ledgerFname)
		}
		if err := f.Sync(); err != nil {
			return nil, y.Wrapf(err, "while syncing %s", ledgerFname)
		}
		l.off = int64(valid)
		opt.Warningf("Truncated %d torn byte(s) from %s at offset %d",
			len(data)-valid, ledgerFname, valid)
	}
	return l, nil
}

// replay folds every valid frame into in-memory state and returns the byte
// offset one past the last valid frame. It is a pure function of the bytes:
// replaying it always converges to the same state.
func (l *gcLedger) replay(data []byte) (int, error) {
	if len(data) < len(ledgerMagic) || !bytes.Equal(data[:len(ledgerMagic)], []byte(ledgerMagic)) {
		return 0, fmt.Errorf("%s has a bad magic header; refusing to open", ledgerFname)
	}
	p := len(ledgerMagic)
	for p < len(data) {
		frameStart := p
		if len(data)-p < ledgerFrameHdr+ledgerCRCLen {
			return frameStart, nil // truncated header
		}
		seq := binary.BigEndian.Uint64(data[p:])
		typ := data[p+8]
		// data[p+9] is flags, reserved.
		payloadLen := int(binary.BigEndian.Uint32(data[p+10:]))
		payloadEnd := p + ledgerFrameHdr + payloadLen
		if payloadLen < 0 || payloadEnd+ledgerCRCLen > len(data) {
			return frameStart, nil // truncated payload
		}
		payload := data[p+ledgerFrameHdr : payloadEnd]
		storedCRC := binary.BigEndian.Uint32(data[payloadEnd:])
		if crc32.Checksum(data[p:payloadEnd], y.CastagnoliCrcTable) != storedCRC {
			return frameStart, nil // corrupt/torn frame
		}
		if err := l.apply(typ, payload); err != nil {
			return frameStart, y.Wrapf(err, "while replaying %s frame at %d", ledgerFname, frameStart)
		}
		l.seq = seq
		l.records++
		p = payloadEnd + ledgerCRCLen
	}
	return p, nil
}

func (l *gcLedger) apply(typ byte, p []byte) error {
	switch typ {
	case recBootstrap:
		l.bootstrapped = true
	case recCreated:
		fid, err := u32p(p, 0)
		if err != nil {
			return err
		}
		l.upsertManaged(fid).origin = originCreated
		l.managed[fid].state = fsAlive
		l.managed[fid].result = gcNone
		l.bumpMaxFid(fid)
	case recAdopted:
		fid, err := u32p(p, 0)
		if err != nil {
			return err
		}
		origin := originLegacy
		if len(p) >= 5 && p[4] == originOrphan {
			origin = originOrphan
		}
		e := l.upsertManaged(fid)
		e.origin = origin
		e.state = fsAlive
		l.bumpMaxFid(fid)
	case recGCStart:
		fid, err := u32p(p, 0)
		if err != nil {
			return err
		}
		attempts, err := u32p(p, 4)
		if err != nil {
			return err
		}
		e, ok := l.managed[fid]
		if !ok {
			return fmt.Errorf("gc-start for unknown fid %d", fid)
		}
		e.state = fsGCRunning
		e.attempts = attempts
		e.result = gcRunning
	case recGCFailed:
		fid, err := u32p(p, 0)
		if err != nil {
			return err
		}
		attempts, err := u32p(p, 4)
		if err != nil {
			return err
		}
		msg, _, err := lString(p[8:])
		if err != nil {
			return err
		}
		e, ok := l.managed[fid]
		if !ok {
			return fmt.Errorf("gc-failed for unknown fid %d", fid)
		}
		e.state = fsAlive // retriable
		e.attempts = attempts
		e.result = gcFailed
		e.lastError = msg
	case recDeleted:
		fid, err := u32p(p, 0)
		if err != nil {
			return err
		}
		e := l.upsertManaged(fid)
		reason := delByGC
		if len(p) >= 5 {
			reason = p[4]
		}
		e.state = fsDeleted
		e.delReason = reason
		if reason == delByGC {
			e.result = gcSucceeded
		}
	case recForeign:
		name, body, err := lString(p)
		if err != nil {
			return err
		}
		size := int64(0)
		reason := foreignUnregistered
		if len(body) >= 8 {
			size = int64(binary.BigEndian.Uint64(body[:8]))
		}
		if len(body) >= 9 {
			reason = body[8]
		}
		l.foreign[name] = &ledgerForeignFile{name: name, size: size, reason: reason}
	default:
		return fmt.Errorf("unknown frame type %d", typ)
	}
	return nil
}

func (l *gcLedger) upsertManaged(fid uint32) *ledgerFile {
	e, ok := l.managed[fid]
	if !ok {
		e = &ledgerFile{fid: fid, state: fsAlive}
		l.managed[fid] = e
	}
	return e
}

func (l *gcLedger) bumpMaxFid(fid uint32) {
	if fid > l.maxFid {
		l.maxFid = fid
	}
}

// appendLocked persists one frame and folds it into memory. In read-only mode
// (or with an unwritable ledger) the frame is only applied in memory.
func (l *gcLedger) appendLocked(typ byte, payload []byte) error {
	if !l.writable {
		return l.apply(typ, payload)
	}
	if l.f == nil {
		f, err := os.OpenFile(filepath.Join(l.dirPath, ledgerFname),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return y.Wrapf(err, "while creating %s", ledgerFname)
		}
		if _, err := f.WriteAt([]byte(ledgerMagic), 0); err != nil {
			_ = f.Close()
			return y.Wrapf(err, "while writing %s magic", ledgerFname)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return y.Wrapf(err, "while syncing %s", ledgerFname)
		}
		if err := syncDir(l.dirPath); err != nil {
			_ = f.Close()
			return y.Wrapf(err, "while syncing value dir")
		}
		l.f = f
		l.off = int64(len(ledgerMagic))
	}

	l.seq++
	frame := make([]byte, ledgerFrameHdr+len(payload)+ledgerCRCLen)
	binary.BigEndian.PutUint64(frame, l.seq)
	frame[8] = typ
	// frame[9] flags reserved.
	binary.BigEndian.PutUint32(frame[10:], uint32(len(payload)))
	copy(frame[ledgerFrameHdr:], payload)
	crc := crc32.Checksum(frame[:ledgerFrameHdr+len(payload)], y.CastagnoliCrcTable)
	binary.BigEndian.PutUint32(frame[ledgerFrameHdr+len(payload):], crc)

	if _, err := l.f.WriteAt(frame, l.off); err != nil {
		return y.Wrapf(err, "while appending to %s", ledgerFname)
	}
	if err := l.f.Sync(); err != nil {
		return y.Wrapf(err, "while syncing %s", ledgerFname)
	}
	l.off += int64(len(frame))
	l.records++
	if err := l.apply(typ, payload); err != nil {
		return err
	}

	if l.records >= ledgerCompactRecords || l.off >= ledgerCompactSize {
		if err := l.compactLocked(); err != nil {
			// Compaction is an optimization; its failure must not lose the
			// durable truth that is already appended.
			l.opt.Warningf("Ledger compaction failed: %v", err)
		}
	}
	return nil
}

// compactLocked atomically rewrites the log as a snapshot of current state.
func (l *gcLedger) compactLocked() error {
	if !l.writable || l.f == nil {
		return nil
	}
	frames, err := l.snapshotFrames()
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(l.dirPath, ".VLOG-LEDGER.compact.*")
	if err != nil {
		return y.Wrapf(err, "while creating ledger temp file")
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	buf := bytes.NewBuffer([]byte(ledgerMagic))
	seq := l.seq - uint64(len(frames))
	if l.seq < uint64(len(frames)) {
		seq = 0
	}
	for _, fr := range frames {
		seq++
		frame := make([]byte, ledgerFrameHdr+len(fr.payload)+ledgerCRCLen)
		binary.BigEndian.PutUint64(frame, seq)
		frame[8] = fr.typ
		binary.BigEndian.PutUint32(frame[10:], uint32(len(fr.payload)))
		copy(frame[ledgerFrameHdr:], fr.payload)
		crc := crc32.Checksum(frame[:ledgerFrameHdr+len(fr.payload)], y.CastagnoliCrcTable)
		binary.BigEndian.PutUint32(frame[ledgerFrameHdr+len(fr.payload):], crc)
		buf.Write(frame)
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		return y.Wrapf(err, "while writing ledger temp file")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return y.Wrapf(err, "while syncing ledger temp file")
	}
	if err := tmp.Close(); err != nil {
		return y.Wrapf(err, "while closing ledger temp file")
	}

	target := filepath.Join(l.dirPath, ledgerFname)
	// Close the old descriptor before renaming: Windows cannot replace a file
	// while any handle to it is open. Failure after this point is rare and
	// leaves the caller an error rather than a silently half-swapped ledger.
	old := l.f
	if old != nil {
		if err := old.Sync(); err != nil {
			return y.Wrapf(err, "while syncing %s before compaction", ledgerFname)
		}
		if err := old.Close(); err != nil {
			return y.Wrapf(err, "while closing %s before compaction", ledgerFname)
		}
		l.f = nil
	}
	if err := os.Rename(tmpName, target); err != nil {
		return y.Wrapf(err, "while replacing %s", ledgerFname)
	}
	if err := syncDir(l.dirPath); err != nil {
		return y.Wrapf(err, "while syncing value dir after ledger compaction")
	}

	f, err := os.OpenFile(target, os.O_RDWR, 0)
	if err != nil {
		return y.Wrapf(err, "while reopening compacted %s", ledgerFname)
	}
	l.f = f
	l.off = int64(buf.Len())
	l.records = len(frames)
	l.seq = seq
	return nil
}

type rawFrame struct {
	typ     byte
	payload []byte
}

// snapshotFrames produces a frame sequence that replays to the current state.
func (l *gcLedger) snapshotFrames() ([]rawFrame, error) {
	var out []rawFrame
	if l.bootstrapped {
		out = append(out, rawFrame{typ: recBootstrap})
	}
	fids := make([]uint32, 0, len(l.managed))
	for fid := range l.managed {
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })
	for _, fid := range fids {
		e := l.managed[fid]
		var origin rawFrame
		if e.origin == originLegacy || e.origin == originOrphan {
			origin = rawFrame{typ: recAdopted, payload: append(putU32(nil, fid), e.origin)}
		} else {
			origin = rawFrame{typ: recCreated, payload: putU32(nil, fid)}
		}
		out = append(out, origin)
		switch e.state {
		case fsGCRunning:
			out = append(out, rawFrame{typ: recGCStart, payload: append(putU32(nil, fid), putU32(nil, e.attempts)...)})
		case fsDeleted:
			payload := putU32(nil, fid)
			payload = append(payload, e.delReason)
			out = append(out, rawFrame{typ: recDeleted, payload: payload})
		}
		if e.state == fsAlive && e.result == gcFailed {
			payload := append(putU32(nil, fid), putU32(nil, e.attempts)...)
			payload = appendString(payload, e.lastError)
			out = append(out, rawFrame{typ: recGCFailed, payload: payload})
		}
	}
	names := make([]string, 0, len(l.foreign))
	for name := range l.foreign {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := l.foreign[name]
		payload := appendString(nil, f.name)
		payload = binary.BigEndian.AppendUint64(payload, uint64(f.size))
		payload = append(payload, f.reason)
		out = append(out, rawFrame{typ: recForeign, payload: payload})
	}
	return out, nil
}

// ---- public (in-package) transitions ---------------------------------------

// ensureBootstrap marks the ledger as having taken ownership of the directory,
// so any *.vlog appearing later that the DB did not create is foreign.
func (l *gcLedger) ensureBootstrap() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bootstrapped {
		return nil
	}
	return l.appendLocked(recBootstrap, nil)
}

func (l *gcLedger) markCreated(fid uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.managed[fid]; ok && e.state != fsDeleted && e.origin != 0 {
		return nil // idempotent
	}
	if err := l.appendLocked(recCreated, putU32(nil, fid)); err != nil {
		return err
	}
	return nil
}

func (l *gcLedger) markAdopted(fid uint32, origin uint8) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.managed[fid]; ok && e.state != fsDeleted {
		return nil
	}
	return l.appendLocked(recAdopted, append(putU32(nil, fid), origin))
}

// gcBegin records the start of a GC attempt. The attempt counter is durable,
// so a retry after a crash is distinguishable in the ledger.
func (l *gcLedger) gcBegin(fid uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.managed[fid]
	if !ok {
		return fmt.Errorf("cannot GC unmanaged fid %d", fid)
	}
	if e.state == fsDeleted {
		return nil // already collected; caller will skip
	}
	e.attempts++
	return l.appendLocked(recGCStart, append(putU32(nil, fid), putU32(nil, e.attempts)...))
}

// gcFailed records a failed (retriable) GC attempt. The file stays alive and
// eligible; the error is kept for the GC report.
func (l *gcLedger) gcFailed(fid uint32, cause error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.managed[fid]
	if !ok {
		return nil
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > ledgerMaxErrMsg {
		msg = msg[:ledgerMaxErrMsg]
	}
	payload := append(putU32(nil, fid), putU32(nil, e.attempts)...)
	payload = appendString(payload, msg)
	return l.appendLocked(recGCFailed, payload)
}

// markDeleted records that a managed file has been unlinked. A deleted fid is
// never reused. Idempotent.
func (l *gcLedger) markDeleted(fid uint32, reason uint8) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.managed[fid]
	if !ok {
		return nil // never managed: do not invent history
	}
	if e.state == fsDeleted {
		return nil
	}
	return l.appendLocked(recDeleted, append(putU32(nil, fid), reason))
}

func (l *gcLedger) markForeign(name string, size int64, reason uint8) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.foreign[name]; ok {
		return nil
	}
	payload := appendString(nil, name)
	payload = binary.BigEndian.AppendUint64(payload, uint64(size))
	payload = append(payload, reason)
	return l.appendLocked(recForeign, payload)
}

// resetInterrupted folds gc-start records without a terminal outcome back to
// alive. Called during open-time reconciliation; purely in-memory.
func (l *gcLedger) resetInterrupted(fid uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.managed[fid]; ok && e.state == fsGCRunning {
		e.state = fsAlive
		e.result = gcNone
	}
}

func (l *gcLedger) state(fid uint32) (ledgerState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.managed[fid]
	if !ok {
		return fsAlive, false
	}
	return e.state, true
}

// eligibleForGC reports whether fid is managed, alive and not already
// collected (or currently being collected).
func (l *gcLedger) eligibleForGC(fid uint32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.managed[fid]
	return ok && e.state == fsAlive
}

func (l *gcLedger) isForeign(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.foreign[name]
	return ok
}

func (l *gcLedger) isBootstrapped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bootstrapped
}

// allTimeMaxFid returns the highest fid ever managed, tombstones included.
func (l *gcLedger) allTimeMaxFid() uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxFid
}

// nextFid is the only fid createVlogFile is allowed to allocate.
func (l *gcLedger) nextFid() uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxFid + 1
}

func (l *gcLedger) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	var err error
	if l.writable {
		err = l.f.Sync()
	}
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// ---- encoding helpers ------------------------------------------------------

func putU32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func u32p(p []byte, off int) (uint32, error) {
	if len(p) < off+4 {
		return 0, fmt.Errorf("short ledger payload: need %d bytes, have %d", off+4, len(p))
	}
	return binary.BigEndian.Uint32(p[off:]), nil
}

func appendString(dst []byte, s string) []byte {
	if len(s) > 1<<16-1 {
		s = s[:1<<16-1]
	}
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(len(s)))
	dst = append(dst, buf[:]...)
	return append(dst, s...)
}

func lString(p []byte) (string, []byte, error) {
	if len(p) < 2 {
		return "", nil, errors.New("short ledger string length")
	}
	n := int(binary.BigEndian.Uint16(p[:2]))
	if len(p) < 2+n {
		return "", nil, errors.New("short ledger string payload")
	}
	return string(p[2 : 2+n]), p[2+n:], nil
}

// ---- read model ------------------------------------------------------------

type ledgerSnapshot struct {
	bootstrapped bool
	managed      []ledgerFile
	foreign      []ledgerForeignFile
}

// snapshot returns point-in-time copies of the folded ledger state.
func (l *gcLedger) snapshot() *ledgerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()

	s := &ledgerSnapshot{bootstrapped: l.bootstrapped}
	for _, e := range l.managed {
		s.managed = append(s.managed, *e)
	}
	sort.Slice(s.managed, func(i, j int) bool { return s.managed[i].fid < s.managed[j].fid })
	for _, f := range l.foreign {
		s.foreign = append(s.foreign, *f)
	}
	sort.Slice(s.foreign, func(i, j int) bool { return s.foreign[i].name < s.foreign[j].name })
	return s
}

// VlogGCLedgerEntry is one file identity in the persistent GC ledger,
// reconciled against whatever is currently on disk.
type VlogGCLedgerEntry struct {
	// Name is the base name inside the value-log directory.
	Name string
	// Fid is the numeric value-log id. It is 0 for foreign files whose names
	// do not parse as a vlog id.
	Fid uint32
	// Managed is true iff this DB owns the file (created it or durably
	// adopted it). Foreign files dropped into the directory by somebody else
	// are never modified or deleted.
	Managed bool
	// State is "alive", "gc_running", "deleted" for managed files and
	// "foreign" for everything else.
	State string
	// Origin is "created", "legacy" (adopted at first ledger boot) or
	// "orphan" (adopted empty next-fid file left by a create crash).
	Origin string
	// GCResult is "none", "running", "failed" or "succeeded".
	GCResult string
	// GCAttempts counts durable GC starts for the file, retries included.
	GCAttempts uint32
	// LastGCError is the error recorded by the last failed, retriable attempt.
	LastGCError string
	// ForeignReason explains why an unowned file was classified foreign:
	// "unregistered", "bad_name", "invalid_vlog" or "reappeared".
	ForeignReason string
	// OnDisk says whether the name currently exists in the value-log dir.
	OnDisk bool
	// Size is the on-disk size when present, -1 otherwise.
	Size int64
	// Drift is true when the ledger conclusion does not match the disk: a
	// managed file recorded alive/running that is missing, or a deleted fid
	// whose file has reappeared. The next open converges both sides (deleting
	// nothing foreign; reclassifying the reappeared file as foreign).
	Drift bool
}

// VlogGCLedgerReport is the persistent ledger folded onto the live directory.
// Every managed and every known-foreign file appears exactly once; *.vlog
// files on disk that the ledger has never classified appear as foreign.
type VlogGCLedgerReport struct {
	Bootstrapped bool
	Entries      []VlogGCLedgerEntry
}

func originString(o uint8) string {
	switch o {
	case originCreated:
		return "created"
	case originLegacy:
		return "legacy"
	case originOrphan:
		return "orphan"
	default:
		return ""
	}
}

func foreignReasonString(r uint8) string {
	switch r {
	case foreignBadName:
		return "bad_name"
	case foreignInvalidVlog:
		return "invalid_vlog"
	case foreignReappeared:
		return "reappeared"
	default:
		return "unregistered"
	}
}

// VlogGCLedgerReport returns the durable value-log GC ledger folded onto the
// current directory contents: which files are owned by this DB, what the GC
// outcome was for each, which directory entries are foreign, and any places
// where the ledger conclusion currently drifts from the disk. It never
// modifies anything.
func (db *DB) VlogGCLedgerReport() (*VlogGCLedgerReport, error) {
	if db.opt.InMemory || db.vlog.ledger == nil {
		return &VlogGCLedgerReport{}, nil
	}
	snap := db.vlog.ledger.snapshot()
	report := &VlogGCLedgerReport{Bootstrapped: snap.bootstrapped}

	onDisk := make(map[string]int64)
	if entries, err := os.ReadDir(db.opt.ValueDir); err == nil {
		for _, de := range entries {
			if de.IsDir() || !strings.HasSuffix(de.Name(), ".vlog") {
				continue
			}
			if info, err := de.Info(); err == nil {
				onDisk[de.Name()] = info.Size()
			}
		}
	}

	known := make(map[string]struct{})
	foreignNames := make(map[string]struct{})
	for _, f := range snap.foreign {
		foreignNames[f.name] = struct{}{}
	}
	for _, e := range snap.managed {
		name := fmt.Sprintf("%06d.vlog", e.fid)
		known[name] = struct{}{}
		// A file sitting at a deleted fid has been reclassified as foreign:
		// the on-disk object is not our historical file, so report the
		// foreign identity only (the managed tombstone is retained internally
		// to guarantee the fid is never reused).
		if e.state == fsDeleted {
			if _, ok := foreignNames[name]; ok {
				continue
			}
		}
		size, present := onDisk[name]
		if !present {
			size = -1
		}
		drift := false
		switch {
		case e.state == fsDeleted && present:
			drift = true
		case (e.state == fsAlive || e.state == fsGCRunning) && !present:
			drift = true
		}
		report.Entries = append(report.Entries, VlogGCLedgerEntry{
			Name:        name,
			Fid:         e.fid,
			Managed:     true,
			State:       e.state.String(),
			Origin:      originString(e.origin),
			GCResult:    e.result.String(),
			GCAttempts:  e.attempts,
			LastGCError: e.lastError,
			OnDisk:      present,
			Size:        size,
			Drift:       drift,
		})
	}
	for _, f := range snap.foreign {
		known[f.name] = struct{}{}
		size, present := onDisk[f.name]
		if !present {
			size = -1
		}
		fid, _ := strconv.ParseUint(strings.TrimSuffix(f.name, ".vlog"), 10, 32)
		report.Entries = append(report.Entries, VlogGCLedgerEntry{
			Name:          f.name,
			Fid:           uint32(fid),
			Managed:       false,
			State:         "foreign",
			ForeignReason: foreignReasonString(f.reason),
			OnDisk:        present,
			Size:          size,
		})
	}
	// Unclassified files physically present: read-only view, no mutation.
	for name, size := range onDisk {
		if _, ok := known[name]; ok {
			continue
		}
		fid, _ := strconv.ParseUint(strings.TrimSuffix(name, ".vlog"), 10, 32)
		report.Entries = append(report.Entries, VlogGCLedgerEntry{
			Name:          name,
			Fid:           uint32(fid),
			Managed:       false,
			State:         "foreign",
			ForeignReason: "unregistered",
			OnDisk:        true,
			Size:          size,
		})
	}
	sort.Slice(report.Entries, func(i, j int) bool { return report.Entries[i].Name < report.Entries[j].Name })
	return report, nil
}
