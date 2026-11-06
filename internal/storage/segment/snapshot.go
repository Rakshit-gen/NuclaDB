// Package segment persists a full HNSW graph to a single on-disk snapshot
// file and restores it back. It exists to make restart fast: without it,
// recovery would mean replaying the entire WAL from empty, re-running full
// graph construction (level assignment + neighbor selection) for every
// vector ever inserted. With it, recovery is: load the last snapshot, then
// replay only the WAL records written after it (see internal/storage/wal).
//
// Loading mmaps the file read-only rather than a buffered ReadFile, so
// decoding walks the page-cache-backed mapping instead of first copying the
// whole file into a heap []byte. Nodes are decoded one at a time straight
// into the graph, so the whole graph must fit in RAM but nothing else of
// its size does.
//
// Format version 2 (magic "NUCLADB2") adds the vector dimension and metric
// name to the header, checked against the caller's config on load, and a
// CRC32 of every preceding byte as a 4-byte trailer. Version 1 files still
// load, with each vector's length checked against the configured dimension.
package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	mmap "github.com/edsrzf/mmap-go"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

var (
	magicV1 = [8]byte{'N', 'U', 'C', 'L', 'A', 'D', 'B', '1'}
	magic   = [8]byte{'N', 'U', 'C', 'L', 'A', 'D', 'B', '2'}
)

const maxSnapshotLevel = 64

// ErrInvalidFormat is returned when a snapshot file's header doesn't match
// the expected magic, indicating it's missing, foreign, or corrupt.
var ErrInvalidFormat = errors.New("segment: invalid or corrupt snapshot format")

// ErrChecksum is returned when a version 2 snapshot's CRC32 trailer doesn't
// match its contents.
var ErrChecksum = errors.New("segment: snapshot checksum mismatch")

// ErrConfigMismatch is returned when a snapshot was written with a
// different vector dimension or metric than the config it is loaded with.
// Loading it anyway would search vectors of the wrong length, or rank them
// with a metric the graph wasn't built for.
var ErrConfigMismatch = errors.New("segment: snapshot dimension or metric differs from config")

func metricName(cfg hnsw.Config) string {
	if cfg.Metric == nil {
		return hnsw.Cosine().Name() // hnsw.New's default
	}
	return cfg.Metric.Name()
}

// Save atomically writes a full snapshot of g to path, tagged with walSeq —
// the WAL sequence number this snapshot reflects, so a later Load knows
// which WAL records (if any) still need replaying on top of it. The write
// goes to a temp file and is renamed into place so a crash mid-write can
// never corrupt a previously-good snapshot.
func Save(path string, g *hnsw.Graph, walSeq uint64) error {
	nodes, entryPoint, maxLevel, hasEntry := g.Snapshot()
	return SaveState(path, g.Config(), nodes, entryPoint, maxLevel, hasEntry, walSeq)
}

// SaveState is Save for graph state the caller already captured with
// hnsw.Graph.Snapshot, so the slow part (writing and fsyncing) can run
// without holding whatever lock kept that capture consistent.
func SaveState(path string, cfg hnsw.Config, nodes []hnsw.NodeState, entryPoint uint64, maxLevel int, hasEntry bool, walSeq uint64) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := writeSnapshot(f, cfg, nodes, entryPoint, maxLevel, hasEntry, walSeq); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// The file itself was fsynced in writeSnapshot; the rename lives in the
	// directory entry, which needs its own fsync to survive a power loss.
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeSnapshot(f *os.File, cfg hnsw.Config, nodes []hnsw.NodeState, entryPoint uint64, maxLevel int, hasEntry bool, walSeq uint64) error {
	sum := crc32.NewIEEE()
	w := bufio.NewWriterSize(io.MultiWriter(f, sum), 1<<20)

	var hasEntryByte byte
	if hasEntry {
		hasEntryByte = 1
	}
	header := make([]byte, 8+8+1+8+4+8)
	copy(header[0:8], magic[:])
	binary.LittleEndian.PutUint64(header[8:16], walSeq)
	header[16] = hasEntryByte
	binary.LittleEndian.PutUint64(header[17:25], entryPoint)
	binary.LittleEndian.PutUint32(header[25:29], uint32(int32(maxLevel)))
	binary.LittleEndian.PutUint64(header[29:37], uint64(len(nodes)))
	name := metricName(cfg)
	header = binary.LittleEndian.AppendUint32(header, uint32(cfg.Dim))
	header = append(header, byte(len(name)))
	header = append(header, name...)
	if _, err := w.Write(header); err != nil {
		return err
	}

	for _, nd := range nodes {
		if err := writeNode(w, nd); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	// The trailer goes straight to f so it isn't part of its own checksum.
	if _, err := f.Write(binary.LittleEndian.AppendUint32(nil, sum.Sum32())); err != nil {
		return err
	}
	return f.Sync()
}

func writeNode(w *bufio.Writer, nd hnsw.NodeState) error {
	var deleted byte
	if nd.Deleted {
		deleted = 1
	}
	fixed := make([]byte, 8+4+1+4)
	binary.LittleEndian.PutUint64(fixed[0:8], nd.ID)
	binary.LittleEndian.PutUint32(fixed[8:12], uint32(int32(nd.Level)))
	fixed[12] = deleted
	binary.LittleEndian.PutUint32(fixed[13:17], uint32(len(nd.Vector)))
	if _, err := w.Write(fixed); err != nil {
		return err
	}
	for _, v := range nd.Vector {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(v))
		if _, err := w.Write(b[:]); err != nil {
			return err
		}
	}

	var levelsBuf [4]byte
	binary.LittleEndian.PutUint32(levelsBuf[:], uint32(len(nd.Neighbors)))
	if _, err := w.Write(levelsBuf[:]); err != nil {
		return err
	}
	for _, layer := range nd.Neighbors {
		var cntBuf [4]byte
		binary.LittleEndian.PutUint32(cntBuf[:], uint32(len(layer)))
		if _, err := w.Write(cntBuf[:]); err != nil {
			return err
		}
		for _, id := range layer {
			var idBuf [8]byte
			binary.LittleEndian.PutUint64(idBuf[:], id)
			if _, err := w.Write(idBuf[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// cursor decodes sequentially from a byte slice backed by an mmap'd
// region, bounds-checking every read against a truncated/corrupt file.
type cursor struct {
	data []byte
	pos  int
}

func (c *cursor) need(n int) error {
	if n < 0 || c.pos < 0 || c.pos > len(c.data) || n > len(c.data)-c.pos {
		return ErrInvalidFormat
	}
	return nil
}

func (c *cursor) skip(n int) error {
	if err := c.need(n); err != nil {
		return err
	}
	c.pos += n
	return nil
}

func (c *cursor) byte() (byte, error) {
	if err := c.need(1); err != nil {
		return 0, err
	}
	b := c.data[c.pos]
	c.pos++
	return b, nil
}

func (c *cursor) uint32() (uint32, error) {
	if err := c.need(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(c.data[c.pos : c.pos+4])
	c.pos += 4
	return v, nil
}

func (c *cursor) uint64() (uint64, error) {
	if err := c.need(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(c.data[c.pos : c.pos+8])
	c.pos += 8
	return v, nil
}

// float32Slice appends n float32s to out.
func (c *cursor) float32Slice(out []float32, n uint32) ([]float32, error) {
	if err := c.need(int(n) * 4); err != nil {
		return out, err
	}
	for range n {
		out = append(out, math.Float32frombits(binary.LittleEndian.Uint32(c.data[c.pos:c.pos+4])))
		c.pos += 4
	}
	return out, nil
}

// uint64Slice appends n uint64s to out.
func (c *cursor) uint64Slice(out []uint64, n uint32) ([]uint64, error) {
	if err := c.need(int(n) * 8); err != nil {
		return out, err
	}
	for range n {
		out = append(out, binary.LittleEndian.Uint64(c.data[c.pos:c.pos+8]))
		c.pos += 8
	}
	return out, nil
}

// Load mmaps the snapshot at path and restores a new Graph built with cfg.
// It returns the WAL sequence number the snapshot reflects. If path does
// not exist, Load returns a fresh empty graph and sequence 0 — the normal
// case for a brand-new database.
func Load(path string, cfg hnsw.Config) (*hnsw.Graph, uint64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return hnsw.New(cfg), 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if fi.Size() == 0 {
		return hnsw.New(cfg), 0, nil
	}

	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = m.Unmap() }()

	c := &cursor{data: m}
	var hdr [8]byte
	if err := c.need(8); err != nil {
		return nil, 0, err
	}
	copy(hdr[:], c.data[:8])
	c.pos = 8
	v2 := hdr == magic
	if !v2 && hdr != magicV1 {
		return nil, 0, ErrInvalidFormat
	}
	if v2 {
		if len(m) < 12 {
			return nil, 0, ErrInvalidFormat
		}
		body := m[:len(m)-4]
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(m[len(m)-4:]) {
			return nil, 0, ErrChecksum
		}
		c.data = body
	}

	walSeq, err := c.uint64()
	if err != nil {
		return nil, 0, err
	}
	hasEntryByte, err := c.byte()
	if err != nil {
		return nil, 0, err
	}
	entryPoint, err := c.uint64()
	if err != nil {
		return nil, 0, err
	}
	maxLevelRaw, err := c.uint32()
	if err != nil {
		return nil, 0, err
	}
	nodeCount, err := c.uint64()
	if err != nil {
		return nil, 0, err
	}
	if hasEntryByte > 1 || nodeCount > uint64(len(c.data)/21) {
		return nil, 0, ErrInvalidFormat
	}
	if v2 {
		dim, err := c.uint32()
		if err != nil {
			return nil, 0, err
		}
		nameLen, err := c.byte()
		if err != nil {
			return nil, 0, err
		}
		if err := c.need(int(nameLen)); err != nil {
			return nil, 0, err
		}
		name := string(c.data[c.pos : c.pos+int(nameLen)])
		c.pos += int(nameLen)
		if int(dim) != cfg.Dim || name != metricName(cfg) {
			return nil, 0, fmt.Errorf("%w: snapshot has dim %d, metric %s; config has dim %d, metric %s",
				ErrConfigMismatch, dim, name, cfg.Dim, metricName(cfg))
		}
	}

	maxLevel := int(int32(maxLevelRaw))
	hasEntry := hasEntryByte == 1
	if maxLevel < 0 || maxLevel > maxSnapshotLevel || hasEntry != (nodeCount > 0) {
		return nil, 0, ErrInvalidFormat
	}

	// Two passes over the mapping: the first collects every id, so the
	// second can resolve neighbor ids to slots as it hands each node to the
	// graph. nd's buffers are reused from node to node, so at no point is
	// there a second copy of the whole graph on the heap.
	nodesAt := c.pos
	ids := make([]uint64, 0, nodeCount)
	for i := uint64(0); i < nodeCount; i++ {
		id, dim, err := skipNode(c)
		if err != nil {
			return nil, 0, fmt.Errorf("segment: decoding node %d: %w", i, err)
		}
		if int(dim) != cfg.Dim {
			return nil, 0, fmt.Errorf("%w: node %d has dim %d, config has %d", ErrConfigMismatch, id, dim, cfg.Dim)
		}
		ids = append(ids, id)
	}
	if c.pos != len(c.data) {
		return nil, 0, ErrInvalidFormat
	}

	g := hnsw.New(cfg)
	if !g.BeginRestore(ids) {
		return nil, 0, ErrInvalidFormat
	}
	c.pos = nodesAt
	var nd hnsw.NodeState
	for range nodeCount {
		if err := readNode(c, &nd); err != nil {
			return nil, 0, err
		}
		if !g.RestoreNode(nd) {
			return nil, 0, ErrInvalidFormat
		}
	}
	if !g.EndRestore(entryPoint, maxLevel, hasEntry) {
		return nil, 0, ErrInvalidFormat
	}
	return g, walSeq, nil
}

// skipNode steps over the next node, returning its id and vector length.
// readNode checks the rest when the node is decoded for real.
func skipNode(c *cursor) (id uint64, dim uint32, err error) {
	if id, err = c.uint64(); err != nil {
		return 0, 0, err
	}
	if err = c.skip(4 + 1); err != nil { // level, deleted
		return 0, 0, err
	}
	if dim, err = c.uint32(); err != nil {
		return 0, 0, err
	}
	if err = c.skip(int(dim) * 4); err != nil {
		return 0, 0, err
	}
	levels, err := c.uint32()
	if err != nil {
		return 0, 0, err
	}
	if levels > maxSnapshotLevel+1 {
		return 0, 0, ErrInvalidFormat
	}
	for range levels {
		cnt, err := c.uint32()
		if err != nil {
			return 0, 0, err
		}
		if err = c.skip(int(cnt) * 8); err != nil {
			return 0, 0, err
		}
	}
	return id, dim, nil
}

// readNode decodes the next node into nd, reusing nd's slices.
func readNode(c *cursor, nd *hnsw.NodeState) error {
	id, err := c.uint64()
	if err != nil {
		return err
	}
	levelRaw, err := c.uint32()
	if err != nil {
		return err
	}
	deletedByte, err := c.byte()
	if err != nil {
		return err
	}
	if deletedByte > 1 {
		return ErrInvalidFormat
	}
	dim, err := c.uint32()
	if err != nil {
		return err
	}
	if nd.Vector, err = c.float32Slice(nd.Vector[:0], dim); err != nil {
		return err
	}
	numLevels, err := c.uint32()
	if err != nil {
		return err
	}
	level := int(int32(levelRaw))
	if level < 0 || level > maxSnapshotLevel || numLevels != uint32(level+1) {
		return ErrInvalidFormat
	}
	for len(nd.Neighbors) < int(numLevels) {
		nd.Neighbors = append(nd.Neighbors, nil)
	}
	nd.Neighbors = nd.Neighbors[:numLevels]
	for l := range nd.Neighbors {
		cnt, err := c.uint32()
		if err != nil {
			return err
		}
		if nd.Neighbors[l], err = c.uint64Slice(nd.Neighbors[l][:0], cnt); err != nil {
			return err
		}
	}
	nd.ID, nd.Level, nd.Deleted = id, level, deletedByte == 1
	return nil
}
