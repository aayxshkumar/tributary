# tributary

A Kafka-compatible message broker written in Go with only the standard library. Real Kafka clients talk to it. The checks use the unmodified kafka-python client, which is vendored so everything runs offline. It speaks the Kafka wire protocol (ApiVersions, Metadata, Produce, Fetch, ListOffsets), stores record batches in per-partition append-only logs, survives restarts and `kill -9`, and has a live dashboard.

The goal is a broker you could swap in for Kafka on one machine: three brokers replicating each partition with leader election and in-sync replicas, consumer groups with rebalancing, idempotent and transactional producers with exactly-once delivery, compaction and retention, and no acknowledged message lost or duplicated when brokers are killed at random, all at hundreds of thousands of messages per second. Today it's one broker, done carefully, with the gaps measured.

```
$ python3 tools/check.py
check                result          value  threshold  measures
produce-consume      PASS                0          0  of 10000 acked messages (kafka-python, acks=all), missing or duplicated on consume
offsets              PASS                0          0  partitions whose acked or consumed offsets aren't 0, 1, 2, ... in order
list-offsets         PASS                0          0  partitions whose beginning/end offsets disagree with what was produced
restart              PASS                0          0  messages missing after a clean broker restart
kill-9               PASS                0          0  of 3000 acked messages, lost after kill -9 of the broker
idempotent-producer  KNOWN GAP           1          0  kafka-python with default settings (enable_idempotence=True) can't produce
torn-tail            KNOWN GAP       0.795          0  fraction of simulated power-loss cuts after which recovery loses or corrupts data

0 failed, 2 known gaps (1.9 s)
```

## Build and run

Needs Go 1.21 or newer and Python 3 (standard library only). No modules to download.

```
go build -o tributary ./cmd/tributary
go test ./...
./tributary serve --port 9092 --dashboard 8093    # dashboard on http://127.0.0.1:8093/
python3 tools/check.py                            # real kafka-python client against it
./tributary crashcheck --trials 200 -v            # torn-write recovery harness
./tributary bench --seconds 5                     # produce throughput over TCP
python3 tools/mutants.py                          # plants bugs, reports what catches each
```

Any Kafka client can connect to `127.0.0.1:9092`. Topics are created on first use, with 3 partitions by default (`--partitions`). With kafka-python, pass `enable_idempotence=False` for now (see the known gaps).

## Protocol

| API | Versions | Notes |
|---|---|---|
| ApiVersions | 0-2 | a newer request gets UNSUPPORTED_VERSION in the v0 format, as Kafka does, and the client retries lower |
| Metadata | 1-4 | v4 is what makes clients treat the broker as 0.11+ and send magic v2 record batches |
| Produce | 3 | acks 0/1/all, CRC-32C checked, offsets assigned by the broker, acks=0 gets no response |
| Fetch | 4 | min_bytes / max_wait, whole batches, at least one batch even if it's over max_bytes |
| ListOffsets | 1 | latest (-1), earliest (-2), by timestamp |

Any other API, or a version outside these ranges, closes the connection, as Kafka does.

## Layout

```
internal/wire      Kafka primitive types, reader with sticky errors, bounded array lengths
internal/record    record batch v2: header, CRC-32C, build/parse, split back-to-back batches
internal/storage   one segment per partition, batch index, read by offset, recovery scan
internal/broker    TCP server, request dispatch, the five APIs, stats
cmd/tributary      serve, crashcheck, bench, the dashboard
tools/check.py     the real-client checks; kafka-python 3.0.13 is in third_party/python (Apache 2.0)
```

## Known gaps

These are measured. They are the roadmap.

1. **Torn writes aren't recovered.** Nothing is fsynced, and recovery trusts the segment file. It scans batch headers until one doesn't fit and stops, but it doesn't check CRCs or truncate the torn tail. New batches go after the garbage, so they read fine until the next restart and are lost after it. In the crash harness 79.5% of simulated power cuts lose or corrupt data. The other 20.5% are cuts that land exactly between batches. `kill -9` passes only because the OS page cache survives a process kill.
2. **No idempotent or transactional producer.** No InitProducerId, no sequence numbers, no producer state, so kafka-python with its default settings can't produce at all.
3. **One broker.** No replication, leader election, ISR or high-watermark semantics across brokers. The high watermark is just the log end offset.
4. **No consumer groups.** No FindCoordinator, JoinGroup, SyncGroup, Heartbeat or OffsetCommit. Consumers must assign partitions themselves and track their own offsets.
5. **One segment per partition, kept forever.** No rolling, retention, compaction or offset index files. Recovery reads every batch header at start-up.
6. **The tests miss one thing.** `tools/mutants.py` plants 11 bugs and 10 are caught. A fetch that ignores `min_bytes` and returns at once survives.

The produce bench reaches about 4.1 M msgs/s (100-byte values, batches of 100, one connection, acks=1) on an M4. Nothing is synced to disk, so that number will drop once durability is real.

## Licence

MIT, see LICENSE. kafka-python (third_party/python) is Apache 2.0 and is only used by the checks.
