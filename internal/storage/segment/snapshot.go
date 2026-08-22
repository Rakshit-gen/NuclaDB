// Package segment persists a full HNSW graph to a single on-disk snapshot
// file and restores it back. It exists to make restart fast: without it,
// recovery would mean replaying the entire WAL from empty, re-running full
// graph construction (level assignment + neighbor selection) for every
// vector ever inserted. With it, recovery is: load the last snapshot, then
// replay only the WAL records written after it (see internal/storage/wal).
//
// Loading mmaps the file read-only rather than a buffered ReadFile, so
// decoding walks the page-cache-backed mapping directly instead of first
// copying the whole file into a heap-allocated []byte — the dataset's
// resident memory during load isn't doubled, and a file larger than
// available RAM is paged in by the OS instead of failing to load.
package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	mmap "github.com/edsrzf/mmap-go"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

var magic = [8]byte{'N', 'U', 'C', 'L', 'A', 'D', 'B', '1'}

// ErrInvalidFormat is returned when a snapshot file's header doesn't match
// the expected magic, indicating it's missing, foreign, or corrupt.
var ErrInvalidFormat = errors.New("segment: invalid or corrupt snapshot format")

// Save atomically writes a full snapshot of g to path, tagged with walSeq —
// the WAL sequence number this snapshot reflects, so a later Load knows
// which WAL records (if any) still need replaying on top of it. The write
// goes to a temp file and is renamed into place so a crash mid-write can
// never corrupt a previously-good snapshot.
func Save(path string, g *hnsw.Graph, walSeq uint64) error {
	nodes, entryPoint, maxLevel, hasEntry := g.Snapshot()

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := writeSnapshot(f, nodes, entryPoint, maxLevel, hasEntry, walSeq); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeSnapshot(f *os.File, nodes []hnsw.NodeState, entryPoint uint64, maxLevel int, hasEntry bool, walSeq uint64) error {
	w := bufio.NewWriterSize(f, 1<<20)

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
	if c.pos+n > len(c.data) {
		return ErrInvalidFormat
	}
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

func (c *cursor) float32Slice(n uint32) ([]float32, error) {
	if err := c.need(int(n) * 4); err != nil {
		return nil, err
	}
	out := make([]float32, n)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(c.data[c.pos : c.pos+4]))
		c.pos += 4
	}
	return out, nil
}

func (c *cursor) uint64Slice(n uint32) ([]uint64, error) {
	if err := c.need(int(n) * 8); err != nil {
		return nil, err
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(c.data[c.pos : c.pos+8])
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
	defer m.Unmap()

	c := &cursor{data: m}
	var hdr [8]byte
	if err := c.need(8); err != nil {
		return nil, 0, err
	}
	copy(hdr[:], c.data[:8])
	c.pos = 8
	if hdr != magic {
		return nil, 0, ErrInvalidFormat
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

	nodes := make([]hnsw.NodeState, 0, nodeCount)
	for i := uint64(0); i < nodeCount; i++ {
		nd, err := readNode(c)
		if err != nil {
			return nil, 0, fmt.Errorf("segment: decoding node %d: %w", i, err)
		}
		nodes = append(nodes, nd)
	}

	g := hnsw.New(cfg)
	g.Restore(nodes, entryPoint, int(int32(maxLevelRaw)), hasEntryByte == 1)
	return g, walSeq, nil
}

func readNode(c *cursor) (hnsw.NodeState, error) {
	id, err := c.uint64()
	if err != nil {
		return hnsw.NodeState{}, err
	}
	levelRaw, err := c.uint32()
	if err != nil {
		return hnsw.NodeState{}, err
	}
	deletedByte, err := c.byte()
	if err != nil {
		return hnsw.NodeState{}, err
	}
	dim, err := c.uint32()
	if err != nil {
		return hnsw.NodeState{}, err
	}
	vector, err := c.float32Slice(dim)
	if err != nil {
		return hnsw.NodeState{}, err
	}
	numLevels, err := c.uint32()
	if err != nil {
		return hnsw.NodeState{}, err
	}
	neighbors := make([][]uint64, numLevels)
	for l := uint32(0); l < numLevels; l++ {
		cnt, err := c.uint32()
		if err != nil {
			return hnsw.NodeState{}, err
		}
		ids, err := c.uint64Slice(cnt)
		if err != nil {
			return hnsw.NodeState{}, err
		}
		neighbors[l] = ids
	}
	return hnsw.NodeState{
		ID:        id,
		Vector:    vector,
		Level:     int(int32(levelRaw)),
		Neighbors: neighbors,
		Deleted:   deletedByte == 1,
	}, nil
}
