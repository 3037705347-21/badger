/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/ristretto/v2/z"

	"github.com/dgraph-io/badger/v4/pb"
)

// Incremental, crash-safe export into a directory.
//
// On-disk layout of an export directory:
//
//	checkpoint                 persistent export checkpoint (owned by Export)
//	badger-export.pid          advisory lock file while an export is running
//	keys/<xx>/<hex-key>        one file per exported live key (owned by Export)
//	.badger-export-staging/    staging area, atomically published on commit
//
// The checkpoint is the single source of truth about which files belong to
// the export. Export only ever creates or removes the files above; any other
// content placed in the directory (including files inside keys/ that do not
// match the managed naming scheme) is left untouched.
//
// All changes of a single Export run are staged first and published in one
// atomic commit (the checkpoint rename is the commit point). A crash or an
// interruption therefore leaves either the previous complete checkpoint or
// the new complete checkpoint, never a half-finished export, and rerunning
// Export converges the directory with the database.
const (
	exportCheckpointFile = "checkpoint"
	exportKeysDir        = "keys"
	exportStagingDir     = ".badger-export-staging"
	exportLockFile       = "badger-export.pid"

	exportCheckpointMagic = "BDXP0001"
	exportKeyFileMagic    = "BDXK0001"
)

// ExportOptions are used to control the incremental export.
type ExportOptions struct {
	// Dir is the export directory. It is created if it does not exist. It must
	// not be the database directory itself.
	Dir string

	// Prefix limits the export to keys with the given prefix. Once an export
	// directory has been initialized with a prefix, every later Export into the
	// same directory must use the same prefix.
	Prefix []byte

	// NumGo is the number of goroutines used to scan the database. A value <= 0
	// uses the database's NumGoroutines option.
	NumGo int
}

// ExportResult summarizes an Export run.
type ExportResult struct {
	// SinceTs is the version watermark the run started from.
	SinceTs uint64
	// NewSinceTs is the version watermark the next run should start from.
	NewSinceTs uint64
	// Full is true if the run rebuilt the whole export instead of applying an
	// incremental delta (first run, or a self-heal after the export directory
	// was found incomplete).
	Full bool
	// Upserts is the number of keys that were added or updated.
	Upserts int
	// Deletions is the number of keys that were removed because the source
	// database no longer contains them.
	Deletions int
	// Keys is the number of live keys contained in the export after the run.
	Keys int
}

// ExportEntry is a single live key/value pair contained in an export directory.
type ExportEntry struct {
	Key       []byte
	Value     []byte
	UserMeta  byte
	Version   uint64
	ExpiresAt uint64
}

// exportMember is the checkpoint record for one exported key.
type exportMember struct {
	version   uint64
	expiresAt uint64
	userMeta  byte
}

// exportCheckpoint is the persistent state of the export.
type exportCheckpoint struct {
	sinceTs uint64
	prefix  []byte
	members map[string]exportMember
}

// exporter carries the state of a single Export run.
type exporter struct {
	db   *DB
	opts ExportOptions

	stageRoot  string // <Dir>/.badger-export-staging
	keysRoot   string // <Dir>/keys
	cpPath     string // <Dir>/checkpoint
	maxVersion uint64
	changes    map[string]*exportChange
	upserts    int

	// Test-only crash injection points. nil in production.
	afterStaging func(numChanges int) error
	afterApply   func() error
}

type exportChange struct {
	deleted bool
	member  exportMember
}

// Export incrementally exports the database into opts.Dir.
//
// The first call to Export performs a full export and writes a persistent
// checkpoint. Later calls only read keys added or modified after the
// checkpoint, remove files for keys that have been deleted or expired at the
// source, and then advance the checkpoint. Rerunning Export after a crash or
// an interruption is safe and brings the export directory back in sync with
// the database. Files that do not belong to the export are never modified or
// removed.
//
// Like Backup, incremental export relies on delete markers and versions still
// being present in the database; do not drop versions (e.g. through aggressive
// GC or database restores) between incremental runs.
//
// Export must not be called on a database in managed mode; use
// DB.ExportAt in that case.
func (db *DB) Export(opts ExportOptions) (ExportResult, error) {
	return newExporter(db, opts).run()
}

func newExporter(db *DB, opts ExportOptions) *exporter {
	return &exporter{
		db:      db,
		opts:    opts,
		changes: make(map[string]*exportChange),
	}
}

func (e *exporter) run() (ExportResult, error) {
	if e.opts.Dir == "" {
		return ExportResult{}, errors.New("Export: Dir must be set")
	}
	if err := os.MkdirAll(e.opts.Dir, 0755); err != nil {
		return ExportResult{}, fmt.Errorf("Export: cannot create directory %q: %w", e.opts.Dir, err)
	}

	// Serialize exporters operating on the same directory. The lock also makes
	// stale staging cleanup safe: while held, no other live exporter exists.
	lock, err := acquireDirectoryLock(e.opts.Dir, exportLockFile, false)
	if err != nil {
		return ExportResult{}, fmt.Errorf("Export: %w", err)
	}
	defer func() { _ = lock.release() }()

	e.keysRoot = filepath.Join(e.opts.Dir, exportKeysDir)
	e.stageRoot = filepath.Join(e.opts.Dir, exportStagingDir)
	e.cpPath = filepath.Join(e.opts.Dir, exportCheckpointFile)

	// Remove staging data left behind by an interrupted previous run.
	if err := os.RemoveAll(e.stageRoot); err != nil {
		return ExportResult{}, fmt.Errorf("Export: cannot clean staging directory: %w", err)
	}

	cp, cpExists, err := readCheckpoint(e.cpPath)
	if err != nil {
		return ExportResult{}, err
	}
	rebuild := !cpExists
	if cpExists {
		if !bytes.Equal(cp.prefix, e.opts.Prefix) {
			return ExportResult{}, fmt.Errorf(
				"Export: checkpoint prefix %q does not match requested prefix %q; "+
					"use a separate export directory per prefix",
				cp.prefix, e.opts.Prefix)
		}
		// Self-heal: if any file owned by the checkpoint has disappeared (an
		// interrupted run or an external deletion), rebuild the whole export so
		// that the result cannot miss key ranges.
		if missing, err := e.membersMissing(cp.members); err != nil {
			return ExportResult{}, err
		} else if missing {
			rebuild = true
		}
	}
	if rebuild {
		cp = &exportCheckpoint{prefix: append([]byte(nil), e.opts.Prefix...)}
	}

	sinceTs := cp.sinceTs

	// Scan the source, staging every delta. Nothing in the committed export is
	// touched until the commit below.
	if err := e.scan(sinceTs); err != nil {
		_ = os.RemoveAll(e.stageRoot)
		return ExportResult{}, err
	}

	if e.afterStaging != nil {
		if err := e.afterStaging(len(e.changes)); err != nil {
			_ = os.RemoveAll(e.stageRoot)
			return ExportResult{}, err
		}
	}

	if len(e.changes) == 0 && !rebuild {
		// Nothing to publish. The checkpoint already describes the directory.
		return ExportResult{
			SinceTs:    sinceTs,
			NewSinceTs: sinceTs,
			Full:       false,
			Keys:       len(cp.members),
		}, nil
	}

	// Build the desired member set by applying the delta to the checkpoint.
	newMembers := make(map[string]exportMember, len(cp.members)+len(e.changes))
	for k, m := range cp.members {
		newMembers[k] = m
	}
	deletions := 0
	for key, ch := range e.changes {
		if ch.deleted {
			if _, ok := newMembers[key]; ok {
				deletions++
			}
			delete(newMembers, key)
			continue
		}
		newMembers[key] = ch.member
	}

	newSince := sinceTs
	if e.maxVersion > newSince {
		newSince = e.maxVersion
	}

	if err := e.commit(newMembers, newSince); err != nil {
		_ = os.RemoveAll(e.stageRoot)
		return ExportResult{}, err
	}

	return ExportResult{
		SinceTs:    sinceTs,
		NewSinceTs: newSince,
		Full:       rebuild,
		Upserts:    e.upserts,
		Deletions:  deletions,
		Keys:       len(newMembers),
	}, nil
}

// scan runs a Stream over versions newer than sinceTs and stages the delta.
func (e *exporter) scan(sinceTs uint64) error {
	stream := e.db.NewStream()
	stream.LogPrefix = "Badger.Export"
	stream.SinceTs = sinceTs
	stream.Prefix = e.opts.Prefix
	if e.opts.NumGo > 0 {
		stream.NumGo = e.opts.NumGo
	}

	// Emit exactly one record per key, describing its newest version. The
	// iterator returns versions newest-first, so the first item seen for a key
	// is its current state: a live value or a delete/expiry marker.
	stream.KeyToList = func(key []byte, itr *Iterator) (*pb.KVList, error) {
		item := itr.Item()
		kv := &pb.KV{
			Key:       item.KeyCopy(nil),
			Version:   item.Version(),
			ExpiresAt: item.ExpiresAt(),
			UserMeta:  []byte{item.UserMeta()},
		}
		if item.IsDeletedOrExpired() {
			kv.Meta = []byte{bitDelete}
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			kv.Value = val
		}

		// Drain the remaining versions of this key. The caller expects the
		// iterator to be positioned at the first item of the next key.
		for itr.Next(); itr.Valid(); itr.Next() {
			if !bytes.Equal(itr.Item().Key(), key) {
				break
			}
		}
		return &pb.KVList{Kv: []*pb.KV{kv}}, nil
	}

	// Send is invoked by a single goroutine.
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		if err != nil {
			return err
		}
		for _, kv := range list.Kv {
			if kv.StreamDone {
				continue
			}
			if kv.Version > e.maxVersion {
				e.maxVersion = kv.Version
			}
			key := string(kv.Key)
			if len(kv.Meta) > 0 && kv.Meta[0]&bitDelete != 0 {
				e.changes[key] = &exportChange{deleted: true}
				continue
			}
			var userMeta byte
			if len(kv.UserMeta) > 0 {
				userMeta = kv.UserMeta[0]
			}
			ch := &exportChange{
				member: exportMember{
					version:   kv.Version,
					expiresAt: kv.ExpiresAt,
					userMeta:  userMeta,
				},
			}
			e.changes[key] = ch
			e.upserts++

			if err := e.stageKeyFile(kv, ch.member); err != nil {
				return err
			}
		}
		return nil
	}

	return stream.Orchestrate(context.Background())
}

// commit publishes the staged run. Every step before the final checkpoint
// rename is idempotent, so a crash anywhere in here is repaired by the next
// run using the previous checkpoint.
func (e *exporter) commit(newMembers map[string]exportMember, newSince uint64) error {
	if err := os.MkdirAll(e.keysRoot, 0755); err != nil {
		return fmt.Errorf("Export: cannot create keys directory: %w", err)
	}

	syncedShards := make(map[string]bool)
	syncShard := func(path string) { syncedShards[path] = true }

	// 1. Publish upserts. os.Rename over an existing file atomically replaces
	// it, so an interrupted rerun simply replaces the same file again.
	for key, ch := range e.changes {
		if ch.deleted {
			continue
		}
		rel := keyRelPath([]byte(key))
		stagePath := filepath.Join(e.stageRoot, rel)
		finalPath := filepath.Join(e.opts.Dir, rel)
		if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
			return fmt.Errorf("Export: cannot create key directory: %w", err)
		}
		if err := os.Rename(stagePath, finalPath); err != nil {
			return fmt.Errorf("Export: cannot publish key file: %w", err)
		}
		syncShard(filepath.Dir(finalPath))
	}

	if e.afterApply != nil {
		if err := e.afterApply(); err != nil {
			return err
		}
	}

	// 2. Converge deletions: files still owned by the previous checkpoint but
	// absent from the new member set. Removing them before the checkpoint
	// rename means a crash triggers the same removal again on rerun.
	for key := range e.changes {
		ch := e.changes[key]
		if !ch.deleted {
			continue
		}
		finalPath := filepath.Join(e.opts.Dir, keyRelPath([]byte(key)))
		if err := os.Remove(finalPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("Export: cannot remove deleted key file: %w", err)
		}
		shard := filepath.Dir(finalPath)
		if _, ok := syncedShards[shard]; !ok {
			syncShard(shard)
		}
	}

	// 3. Reconcile the managed subtree with the new member set. This removes
	// leftovers of interrupted runs but never touches files outside the
	// managed naming scheme, so externally added content is preserved.
	if err := reconcileKeysDir(e.keysRoot, newMembers); err != nil {
		return err
	}

	// 4. Persist directory entries before committing the checkpoint. Shards
	// emptied by deletions have just been removed by reconcile, so missing
	// directories are expected here; their removal is persisted by the keys
	// directory sync below.
	for shard := range syncedShards {
		if _, err := os.Stat(shard); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		if err := syncDir(shard); err != nil {
			return err
		}
	}
	if err := syncDir(e.keysRoot); err != nil {
		return err
	}

	// 5. The commit point: atomically replace the checkpoint, then sync the
	// export directory so the rename is durable.
	newCP := &exportCheckpoint{
		sinceTs: newSince,
		prefix:  append([]byte(nil), e.opts.Prefix...),
		members: newMembers,
	}
	tmpPath := e.cpPath + ".tmp"
	if err := writeFileSync(tmpPath, marshalCheckpoint(newCP), 0644); err != nil {
		return fmt.Errorf("Export: cannot write checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, e.cpPath); err != nil {
		return fmt.Errorf("Export: cannot commit checkpoint: %w", err)
	}
	if err := syncDir(e.opts.Dir); err != nil {
		return err
	}

	// 6. Drop the staging area. If this is interrupted, the next run removes
	// the well-known staging directory before doing anything else.
	if err := os.RemoveAll(e.stageRoot); err != nil {
		return fmt.Errorf("Export: cannot remove staging directory: %w", err)
	}
	return syncDir(e.opts.Dir)
}

// stageKeyFile writes one upsert into the staging area and fsyncs it so the
// later rename into the keys directory publishes durable content.
func (e *exporter) stageKeyFile(kv *pb.KV, m exportMember) error {
	rel := keyRelPath(kv.Key)
	stagePath := filepath.Join(e.stageRoot, rel)
	if err := os.MkdirAll(filepath.Dir(stagePath), 0755); err != nil {
		return fmt.Errorf("Export: cannot create staging directory: %w", err)
	}
	data := marshalKeyFile(kv, m)
	if err := writeFileSync(stagePath, data, 0644); err != nil {
		return fmt.Errorf("Export: cannot stage key %x: %w", kv.Key, err)
	}
	return syncDir(filepath.Dir(stagePath))
}

// membersMissing reports whether any file owned by the checkpoint is absent.
func (e *exporter) membersMissing(members map[string]exportMember) (bool, error) {
	for key := range members {
		path := filepath.Join(e.opts.Dir, keyRelPath([]byte(key)))
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return true, nil
			}
			return false, fmt.Errorf("Export: cannot read export directory: %w", err)
		}
	}
	return false, nil
}

// reconcileKeysDir removes files under the managed keys subtree that are not
// part of the member set and then removes shard directories left empty.
// Files and directories that do not match the managed naming scheme are left
// untouched.
func reconcileKeysDir(keysRoot string, members map[string]exportMember) error {
	shards, err := os.ReadDir(keysRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("Export: cannot read keys directory: %w", err)
	}
	for _, shard := range shards {
		if !shard.IsDir() || !isHex(shard.Name()) || len(shard.Name()) != 2 {
			continue
		}
		shardPath := filepath.Join(keysRoot, shard.Name())
		files, err := os.ReadDir(shardPath)
		if err != nil {
			return fmt.Errorf("Export: cannot read shard directory: %w", err)
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !isManagedFileName(shard.Name(), name) {
				continue
			}
			key, err := hex.DecodeString(name)
			if err != nil {
				continue // Does not match the managed scheme; leave it alone.
			}
			if _, ok := members[string(key)]; ok {
				continue
			}
			if err := os.Remove(filepath.Join(shardPath, name)); err != nil &&
				!errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("Export: cannot remove stale key file: %w", err)
			}
		}
		// Remove the shard directory only if it is now empty. External content
		// inside it keeps it alive.
		if files, err := os.ReadDir(shardPath); err == nil && len(files) == 0 {
			if err := os.Remove(shardPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("Export: cannot remove empty shard directory: %w", err)
			}
		}
	}
	return nil
}

// keyRelPath returns the export-relative path of a key file: keys/<xx>/<hex>,
// sharded by the first key byte to avoid oversized flat directories.
func keyRelPath(key []byte) string {
	h := hex.EncodeToString(key)
	return filepath.Join(exportKeysDir, h[:2], h)
}

// isManagedFileName reports whether name is the file name of a managed key in
// shard: an even-length hex string of at least two characters that starts
// with the shard name.
func isManagedFileName(shard, name string) bool {
	if len(name) < 2 || len(name)%2 != 0 || name[:2] != shard {
		return false
	}
	return isHex(name)
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return len(s) > 0
}

func writeFileSync(path string, data []byte, perm fs.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if err := f.Sync(); werr == nil {
		werr = err
	}
	if err := f.Close(); werr == nil {
		werr = err
	}
	return werr
}

// ----------------------------------------------------------------------------
// Checkpoint encoding
//
//	magic (8 bytes) | prefixLen u32 | prefix | sinceTs u64 | count u64 |
//	records: keyLen u32 | key | version u64 | expiresAt u64 | userMeta u8 |
//	crc32 u32 over everything before it
// ----------------------------------------------------------------------------

func marshalCheckpoint(cp *exportCheckpoint) []byte {
	keys := make([]string, 0, len(cp.members))
	for k := range cp.members {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := make([]byte, 0, 64)
	b = append(b, exportCheckpointMagic...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(cp.prefix)))
	b = append(b, cp.prefix...)
	b = binary.LittleEndian.AppendUint64(b, cp.sinceTs)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(keys)))
	for _, k := range keys {
		m := cp.members[k]
		b = binary.LittleEndian.AppendUint32(b, uint32(len(k)))
		b = append(b, k...)
		b = binary.LittleEndian.AppendUint64(b, m.version)
		b = binary.LittleEndian.AppendUint64(b, m.expiresAt)
		b = append(b, m.userMeta)
	}
	b = binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	return b
}

func readCheckpoint(path string) (*exportCheckpoint, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("Export: cannot read checkpoint: %w", err)
	}
	cp, err := unmarshalCheckpoint(data)
	if err != nil {
		return nil, false, fmt.Errorf("Export: checkpoint at %q is corrupt: %w; "+
			"remove it and run a full export into an empty directory", path, err)
	}
	return cp, true, nil
}

func unmarshalCheckpoint(data []byte) (*exportCheckpoint, error) {
	const headerLen = len(exportCheckpointMagic) + 4
	if len(data) < headerLen+4 {
		return nil, errors.New("file too short")
	}
	if string(data[:len(exportCheckpointMagic)]) != exportCheckpointMagic {
		return nil, errors.New("bad magic")
	}
	body, trailer := data[:len(data)-4], data[len(data)-4:]
	if binary.LittleEndian.Uint32(trailer) != crc32.ChecksumIEEE(body) {
		return nil, errors.New("checksum mismatch")
	}

	r := newByteReader(data[len(exportCheckpointMagic):])
	prefixLen, err := r.uint32()
	if err != nil {
		return nil, err
	}
	prefix, err := r.bytes(int(prefixLen))
	if err != nil {
		return nil, err
	}
	sinceTs, err := r.uint64()
	if err != nil {
		return nil, err
	}
	count, err := r.uint64()
	if err != nil {
		return nil, err
	}
	members := make(map[string]exportMember, count)
	for i := uint64(0); i < count; i++ {
		keyLen, err := r.uint32()
		if err != nil {
			return nil, err
		}
		key, err := r.bytes(int(keyLen))
		if err != nil {
			return nil, err
		}
		version, err := r.uint64()
		if err != nil {
			return nil, err
		}
		expiresAt, err := r.uint64()
		if err != nil {
			return nil, err
		}
		userMeta, err := r.byte()
		if err != nil {
			return nil, err
		}
		members[string(key)] = exportMember{
			version:   version,
			expiresAt: expiresAt,
			userMeta:  userMeta,
		}
	}
	if r.remaining() != 4 {
		return nil, errors.New("trailing bytes in checkpoint")
	}
	return &exportCheckpoint{
		sinceTs: sinceTs,
		prefix:  prefix,
		members: members,
	}, nil
}

// ----------------------------------------------------------------------------
// Key file encoding
//
//	magic (8) | userMeta u8 | version u64 | expiresAt u64 |
//	keyLen u32 | key | valueLen u64 | value | crc32 u32
// ----------------------------------------------------------------------------

func marshalKeyFile(kv *pb.KV, m exportMember) []byte {
	b := make([]byte, 0, len(exportKeyFileMagic)+1+8+8+4+len(kv.Key)+8+len(kv.Value)+4)
	b = append(b, exportKeyFileMagic...)
	b = append(b, m.userMeta)
	b = binary.LittleEndian.AppendUint64(b, m.version)
	b = binary.LittleEndian.AppendUint64(b, m.expiresAt)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(kv.Key)))
	b = append(b, kv.Key...)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(kv.Value)))
	b = append(b, kv.Value...)
	b = binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	return b
}

func unmarshalKeyFile(data []byte) (ExportEntry, error) {
	var e ExportEntry
	const fixedLen = len(exportKeyFileMagic) + 1 + 8 + 8
	if len(data) < fixedLen+4 {
		return e, errors.New("key file too short")
	}
	if string(data[:len(exportKeyFileMagic)]) != exportKeyFileMagic {
		return e, errors.New("bad magic")
	}
	if binary.LittleEndian.Uint32(data[len(data)-4:]) !=
		crc32.ChecksumIEEE(data[:len(data)-4]) {
		return e, errors.New("checksum mismatch")
	}

	r := newByteReader(data[len(exportKeyFileMagic):])
	userMeta, err := r.byte()
	if err != nil {
		return e, err
	}
	version, err := r.uint64()
	if err != nil {
		return e, err
	}
	expiresAt, err := r.uint64()
	if err != nil {
		return e, err
	}
	keyLen, err := r.uint32()
	if err != nil {
		return e, err
	}
	key, err := r.bytes(int(keyLen))
	if err != nil {
		return e, err
	}
	valueLen, err := r.uint64()
	if err != nil {
		return e, err
	}
	value, err := r.bytes(int(valueLen))
	if err != nil {
		return e, err
	}
	if r.remaining() != 4 {
		return e, errors.New("trailing bytes in key file")
	}
	return ExportEntry{
		Key:       key,
		Value:     value,
		UserMeta:  userMeta,
		Version:   version,
		ExpiresAt: expiresAt,
	}, nil
}

// byteReader is a bounds-checked little-endian reader. All methods return
// copies of the underlying data, safe to retain.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader { return &byteReader{data: data} }

func (r *byteReader) remaining() int { return len(r.data) - r.pos }

func (r *byteReader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, errors.New("unexpected end of export data")
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *byteReader) uint32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, errors.New("unexpected end of export data")
	}
	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *byteReader) uint64() (uint64, error) {
	if r.remaining() < 8 {
		return 0, errors.New("unexpected end of export data")
	}
	v := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *byteReader) bytes(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, errors.New("unexpected end of export data")
	}
	v := append([]byte(nil), r.data[r.pos:r.pos+n]...)
	r.pos += n
	return v, nil
}

// ReadExport iterates over the live keys contained in an export directory and
// invokes fn for each one, in lexicographical key order. An empty (or not yet
// initialized) export directory yields no calls. fn must not retain the slices
// passed to it; copy them if needed.
func ReadExport(dir string, fn func(ExportEntry) error) error {
	keysRoot := filepath.Join(dir, exportKeysDir)
	shards, err := os.ReadDir(keysRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("ReadExport: cannot read keys directory: %w", err)
	}

	var paths []string
	for _, shard := range shards {
		if !shard.IsDir() || !isHex(shard.Name()) || len(shard.Name()) != 2 {
			continue
		}
		files, err := os.ReadDir(filepath.Join(keysRoot, shard.Name()))
		if err != nil {
			return fmt.Errorf("ReadExport: cannot read shard directory: %w", err)
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !isManagedFileName(shard.Name(), name) {
				continue
			}
			paths = append(paths, filepath.Join(keysRoot, shard.Name(), name))
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		// Paths sort identically to their decoded keys because shard and file
		// names are fixed-width-prefix hex encodings of the keys.
		pi, pj := paths[i], paths[j]
		return filepath.Base(pi) < filepath.Base(pj)
	})

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("ReadExport: %w", err)
		}
		entry, err := unmarshalKeyFile(data)
		if err != nil {
			return fmt.Errorf("ReadExport: key file %q is corrupt: %w", path, err)
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}
