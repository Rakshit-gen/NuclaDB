package pq

import "math/rand"

// kmeans clusters points into k centroids via Lloyd's algorithm,
// initialized with k-means++ for better convergence than random init, and
// returns a flattened k*subDim centroid matrix.
func kmeans(points [][]float32, k, iters int, rng *rand.Rand) []float32 {
	subDim := len(points[0])
	centroids := kmeansPlusPlusInit(points, k, rng)
	assign := make([]int, len(points))

	for iter := 0; iter < iters; iter++ {
		changed := false
		for i, p := range points {
			best, bestDist := 0, sqDist(p, centroidAt(centroids, 0, subDim))
			for c := 1; c < k; c++ {
				d := sqDist(p, centroidAt(centroids, c, subDim))
				if d < bestDist {
					bestDist = d
					best = c
				}
			}
			if assign[i] != best {
				changed = true
				assign[i] = best
			}
		}

		sums := make([][]float64, k)
		counts := make([]int, k)
		for c := range sums {
			sums[c] = make([]float64, subDim)
		}
		for i, p := range points {
			c := assign[i]
			counts[c]++
			for d := 0; d < subDim; d++ {
				sums[c][d] += float64(p[d])
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				continue // empty cluster: leave its centroid where it was
			}
			dst := centroidAt(centroids, c, subDim)
			for d := 0; d < subDim; d++ {
				dst[d] = float32(sums[c][d] / float64(counts[c]))
			}
		}

		if !changed && iter > 0 {
			break
		}
	}
	return centroids
}

// kmeansPlusPlusInit seeds k centroids by repeatedly picking a training
// point with probability proportional to its squared distance from the
// nearest centroid chosen so far — spreads initial centroids out instead
// of risking several landing in the same cluster, which plain random init
// can do and which slows or degrades convergence.
func kmeansPlusPlusInit(points [][]float32, k int, rng *rand.Rand) []float32 {
	subDim := len(points[0])
	flat := make([]float32, k*subDim)

	first := rng.Intn(len(points))
	copy(flat[0:subDim], points[first])

	minDist2 := make([]float64, len(points))
	for i, p := range points {
		minDist2[i] = float64(sqDist(p, points[first]))
	}

	for c := 1; c < k; c++ {
		var total float64
		for _, d := range minDist2 {
			total += d
		}

		var idx int
		if total == 0 {
			// Every remaining point coincides with an already-chosen
			// centroid; fall back to uniform pick rather than dividing by
			// zero.
			idx = rng.Intn(len(points))
		} else {
			r := rng.Float64() * total
			var cum float64
			for i, d := range minDist2 {
				cum += d
				if cum >= r {
					idx = i
					break
				}
			}
		}

		copy(flat[c*subDim:(c+1)*subDim], points[idx])
		for i, p := range points {
			d := float64(sqDist(p, points[idx]))
			if d < minDist2[i] {
				minDist2[i] = d
			}
		}
	}
	return flat
}
