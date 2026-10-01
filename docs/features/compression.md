# Compression (not in the server yet)

Source: [`internal/index/pq`](../../internal/index/pq)

This is a finished and tested Go package, but `nucladbd` does not use it
yet. A server you start today stores full vectors.

## The idea

A vector of 64 numbers takes 256 bytes. Product quantization (PQ) cuts it
into 16 short pieces and replaces each piece with the number of the
closest match from a small, learned list of 256 typical pieces. Each
number fits in one byte, so the whole vector becomes 16 bytes: 16 times
smaller, always, whatever the data.

The price is accuracy, because each piece is now an approximation.

## Measured

3,000 random vectors of 64 numbers, 16 bytes per vector (16 times
smaller), checked against exact search:

| Method | Right answers (recall@10) |
|---|---|
| Compressed codes only | 57.7% |
| Codes pick the best 50, then re-check them with full vectors | 96.0% |
| Re-check the best 100 | 99.3% |
| Re-check the best 200 | 100% |

So compression alone gets a bit more than half right, but using it as a
fast first pass and checking the top 100 with the real vectors gets 99.3%.
That works well when the full vectors live somewhere cheap, like a file on
disk, and only the small codes stay in memory.

## Skipping most of the codes (IVF)

Even small codes are slow to scan one by one. `pq.IVFIndex` first sorts
vectors into groups and only scans the groups nearest the question.

5,000 clustered vectors, 32 groups, re-checking the top 100:

| Groups scanned | Share of data scanned | Right answers |
|---|---|---|
| 1 | about 1/32 | 63.7% |
| 4 | about 1/8 | 87.0% |
| 8 | about 1/4 | 94.3% |
| 32 | all | 99.3% |

More detail: [What product quantization cost](../writeups/03-product-quantization-cost.md).
