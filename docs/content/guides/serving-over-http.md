---
title: "Serving search over HTTP"
description: "Run tatami serve over a directory of search segments: a concurrent lock-free broker with admission control, a per-request deadline, and a smart segment cache sized to hold the working set."
weight: 70
---

`tatami serve` puts an HTTP server in front of a broker so a directory of search segments answers queries over the network. It is built to handle thousands of concurrent queries on one process while keeping the latency budget and bounding the memory.

## Start the server

Point it at a directory of `.tatami` search segments:

```bash
tatami serve ./segments --addr :8080
```

```
opening 254 shards under ./segments
serving 20246 docs across 254 shards on :8080 (cache=64, max-in-flight=256)
```

It globs the top-level `*.tatami` files in sorted order so the shard ids stay stable across restarts, builds a routing index across them, and opens a broker that keeps only a bounded working set of segments resident.

## Query it

`GET /search` takes a query `q` and an optional result count `k`:

```bash
curl 'localhost:8080/search?q=open+source+software&k=10'
```

```json
{
  "query": "open source software",
  "k": 10,
  "total": 10,
  "took_ms": 1.4,
  "stats": { "candidates": 251, "visited": 83, "threshold": 7.21 },
  "results": [
    { "doc_id": "4f1e...", "url": "https://example.com/a", "title": "An example", "snippet": "open source ...", "score": 9.83 }
  ]
}
```

`took_ms` is the wall time the broker spent, the number the ten-millisecond target is read against, and `stats` shows how few of the candidate shards the answer actually touched. Two more endpoints round it out: `GET /healthz` is a plain `200` liveness probe a load balancer polls, and `GET /stats` reports the broker shape and the serving counters.

```bash
curl localhost:8080/stats
```

```json
{
  "shards": 254, "docs": 20246, "cache_len": 64,
  "max_in_flight": 256, "in_flight": 3,
  "total": 10432, "rejected": 0, "timed_out": 0, "canceled": 0, "failed": 0
}
```

## The broker answers without a lock

net/http already gives one goroutine per request, and the broker is safe to call from all of them at once: it routes, prunes, and scores against a reference-counted concurrent segment cache, so two queries that touch different shards never wait on each other. A query is never serialised behind another query's work, so the tail latency under load is the cost of one query, not the sum of the queue ahead of it. A concurrent answer is identical to the single-threaded one: the same shards, the same scores, the same ranking.

## A smart cache keeps the working set warm

The latency depends on one thing above all: the segments a query needs are resident. A warm query runs the posting walk and the forward-column read from memory; a cold one pays to decode a whole inverted region first. The working set is the union of the shards the queries *visit* during routing, which is larger than the set that produces a top result, and a cache below it thrashes. Size `--cache` to hold the working set:

```bash
tatami serve ./segments --cache 254
```

Sized to the working set, a cycled query mix that thrashed at a smaller cap runs an order of magnitude faster. The cache cap also bounds the memory: the open-file and decoded-index footprint is the cap, not the shard count, so one process serves a shard count it could never hold open at once.

## Admission control and a deadline bound a burst

Goroutine-per-request is unbounded by default: a flood of clients spawns a goroutine and an in-flight query each, and the memory grows with the flood. Two limits fix a ceiling that does not move with load.

- **Admission control.** A counting semaphore caps the queries running at once. An arrival past the cap gets a `503` immediately rather than queuing without bound, so the CPU and memory a burst can claim are bounded by `--max-in-flight`, not by the arrival rate. The slot is held until the search actually finishes, so the cap bounds work and not just connections.
- **A per-request deadline.** A query that stalls on a cold-shard read returns `504` after `--timeout` rather than tying up a slot indefinitely. The deadline is generous next to the sub-millisecond warm path, so it fires only on a real stall.

Together with the cache cap, these bound both the per-query memory and the resident memory, so a flood degrades into sheds and timeouts rather than into unbounded growth.

## Tune it

| Flag | Default | When to change it |
|------|---------|-------------------|
| `--cache` | `64` | Raise it to hold the working set the queries touch, the single biggest latency lever |
| `--max-in-flight` | `256` | Lower it to protect a small box, raise it on a big one with headroom |
| `--timeout` | `2s` | Lower it to fail a stalled query faster, raise it if cold shards are genuinely slow |
| `--max-k` | `100` | Cap how large a result set a single request may force |
| `--default-k` | `10` | The result count when a request omits `k` |

## Latency under load

On a real shard split into 254 shards with the working set warm, driven at one in-flight query per core:

| Class | p50 | p90 | p99 | Throughput |
|-------|-----|-----|-----|------------|
| Single keyword | ~140 us | ~340 us | ~1.4 ms | over 31,000 qps |
| Multi-term phrase | ~1.0 ms | ~6 ms | ~28 ms | ~3,200 qps |

Single-keyword serving, the class the ten-millisecond target is stated against, holds a p99 well under the budget at over thirty thousand queries per second. Multi-term phrases are a heavier multi-list traversal; their median stays well inside the budget and their tail is bounded by admission and the deadline rather than left to run away. Through five thousand concurrent queries the resident segment count holds at the cache cap, and the ranking stays exact.

## Scale past one process

One `Server` drives one broker over the shards one machine can hold. To scale past that, run several servers behind a load balancer, or front several brokers with an [aggregator](/guides/distributed-serving/) so one query reaches a whole fleet of shards.

## Where to go next

- For the broker, the routing index, search-only segments, and the aggregator tier, see [distributed serving at scale](/guides/distributed-serving/).
- For every flag and endpoint, see the [CLI reference](/reference/cli/).
