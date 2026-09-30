package hnsw

// candidate is a node reachable during graph traversal, paired with its
// distance to the current query vector.
type candidate struct {
	id   uint64
	dist float32
}

// minHeap pops the closest candidate first; used for the traversal frontier.
// maxHeap pops the farthest first; used to keep a bounded best-so-far set,
// evicting the worst entry as better ones arrive.
//
// These are typed binary heaps rather than container/heap: its Push and Pop
// take interface{}, which boxed every candidate into its own allocation on
// the search hot path.
type (
	minHeap []candidate
	maxHeap []candidate
)

func newMinHeap() *minHeap { return &minHeap{} }
func newMaxHeap() *maxHeap { return &maxHeap{} }

func (h minHeap) Len() int { return len(h) }
func (h maxHeap) Len() int { return len(h) }

func (h *minHeap) push(c candidate) { *h = append(*h, c); up(*h, len(*h)-1, closer) }
func (h *maxHeap) push(c candidate) { *h = append(*h, c); up(*h, len(*h)-1, farther) }
func (h *minHeap) pop() candidate   { return pop((*[]candidate)(h), closer) }
func (h *maxHeap) pop() candidate   { return pop((*[]candidate)(h), farther) }

func closer(a, b candidate) bool  { return a.dist < b.dist }
func farther(a, b candidate) bool { return a.dist > b.dist }

func up(h []candidate, i int, before func(a, b candidate) bool) {
	for i > 0 {
		p := (i - 1) / 2
		if !before(h[i], h[p]) {
			return
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func pop(hp *[]candidate, before func(a, b candidate) bool) candidate {
	h := *hp
	top := h[0]
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	i := 0
	for {
		l, best := 2*i+1, i
		if l < n && before(h[l], h[best]) {
			best = l
		}
		if r := l + 1; r < n && before(h[r], h[best]) {
			best = r
		}
		if best == i {
			break
		}
		h[i], h[best] = h[best], h[i]
		i = best
	}
	*hp = h
	return top
}
