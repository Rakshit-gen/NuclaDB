// Package bench measures NuclaDB's actual recall/QPS/memory on the
// standard SIFT ANN-benchmark dataset, both standalone and head-to-head
// against a real Qdrant instance over its own API — no numbers here are
// estimated.
package bench

import (
	"encoding/binary"
	"fmt"
	"os"
)

// LoadFvecs reads the .fvecs format used by the texmex/SIFT corpus: a
// sequence of records, each a little-endian int32 dimension followed by
// that many little-endian float32 values.
func LoadFvecs(path string) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var vectors [][]float32
	for {
		var dim int32
		if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
			break // EOF: normal end of a well-formed file
		}
		raw := make([]float32, dim)
		if err := binary.Read(f, binary.LittleEndian, &raw); err != nil {
			return nil, fmt.Errorf("bench: truncated vector in %s: %w", path, err)
		}
		vectors = append(vectors, raw)
	}
	return vectors, nil
}

// LoadIvecs reads the .ivecs format (identical framing to .fvecs, int32
// payload instead of float32) used for SIFT's precomputed groundtruth:
// groundtruth[i] holds the true nearest-neighbor ids for query i, ranked
// closest-first.
func LoadIvecs(path string) ([][]int32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows [][]int32
	for {
		var dim int32
		if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
			break
		}
		raw := make([]int32, dim)
		if err := binary.Read(f, binary.LittleEndian, &raw); err != nil {
			return nil, fmt.Errorf("bench: truncated row in %s: %w", path, err)
		}
		rows = append(rows, raw)
	}
	return rows, nil
}
