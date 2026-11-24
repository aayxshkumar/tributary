"""Check tributary with a real Kafka client and its own crash harness.

kafka-python is vendored as a wheel in third_party/python (used only here),
so this runs offline. Starts a broker on a free port with a temp data dir.

    python3 tools/check.py [--binary ./tributary]
"""
import argparse
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WHEELS = os.path.join(ROOT, "third_party", "python")
for name in sorted(os.listdir(WHEELS)):
    if name.endswith(".whl"):
        sys.path.insert(0, os.path.join(WHEELS, name))

from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # noqa: E402

logging.getLogger("kafka").setLevel(logging.CRITICAL)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Broker:
    def __init__(self, binary, data):
        self.binary, self.data = binary, data
        self.port = free_port()
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen([self.binary, "serve", "--port", str(self.port), "--dashboard", "0", "--data", self.data,
                                      "--partitions", "3"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("broker did not start")

    def stop(self, kill=False):
        if self.proc:
            self.proc.kill() if kill else self.proc.terminate()
            self.proc.wait()
            self.proc = None

    @property
    def addr(self):
        return f"127.0.0.1:{self.port}"


def consume_all(addr, topic, parts, expect, timeout=15):
    c = KafkaConsumer(bootstrap_servers=addr, group_id=None, enable_auto_commit=False, consumer_timeout_ms=3000)
    tps = [TopicPartition(topic, p) for p in range(parts)]
    c.assign(tps)
    c.seek_to_beginning(*tps)
    got = []
    deadline = time.time() + timeout
    while len(got) < expect and time.time() < deadline:
        batch = c.poll(timeout_ms=500)
        for tp, msgs in batch.items():
            for m in msgs:
                got.append((tp.partition, m.offset, m.key, m.value))
    c.close()
    return got


class Table:
    def __init__(self):
        self.rows = []

    def add(self, name, value, threshold, measures, gap=False):
        ok = value <= threshold
        self.rows.append((name, "PASS" if ok else ("KNOWN GAP" if gap else "FAIL"), value, threshold, measures))

    def print(self, secs):
        print(f"{'check':<20} {'result':<10} {'value':>10} {'threshold':>10}  measures")
        for n, r, v, t, m in self.rows:
            print(f"{n:<20} {r:<10} {v:>10.6g} {t:>10.6g}  {m}")
        failed = sum(1 for r in self.rows if r[1] == "FAIL")
        print(f"\n{failed} failed, {sum(1 for r in self.rows if r[1] == 'KNOWN GAP')} known gaps ({secs:.1f} s)")
        return failed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=os.path.join(ROOT, "tributary"))
    args = ap.parse_args()
    binary = os.path.abspath(args.binary)
    if not os.path.exists(binary):
        print(f"no binary at {binary}; run: go build -o tributary ./cmd/tributary")
        return 2
    t0 = time.time()
    table = Table()
    tmp = tempfile.mkdtemp(prefix="tributary-check-")
    b = Broker(binary, os.path.join(tmp, "data"))
    try:
        b.start()
        n = 10000
        # 1. Produce with the real client (acks=all, keyed, explicit partitions), then consume everything.
        p = KafkaProducer(bootstrap_servers=b.addr, acks="all", enable_idempotence=False, linger_ms=5)
        futures = [p.send("orders", key=f"k{i}".encode(), value=f"order-{i}".encode(), partition=i % 3) for i in range(n)]
        p.flush()
        acked = [f.get(timeout=10) for f in futures]
        p.close()
        got = consume_all(b.addr, "orders", 3, n)
        want = sorted((i % 3, f"k{i}".encode(), f"order-{i}".encode()) for i in range(n))
        have = sorted((part, k, v) for part, _, k, v in got)
        missing = len(set(want) - set(have)) + (len(have) - len(set(have)))
        table.add("produce-consume", missing, 0, f"of {n} acked messages (kafka-python, acks=all), missing or duplicated on consume")
        # Offsets reported to the producer match what the consumer sees, per partition, in order.
        bad = 0
        for part in range(3):
            offs = [m.offset for m in acked if m.partition == part]
            if offs != list(range(len(offs))):
                bad += 1
            seen = [o for pp, o, _, _ in sorted(got) if pp == part]
            if seen != sorted(seen) or seen != list(range(len(seen))):
                bad += 1
        table.add("offsets", bad, 0, "partitions whose acked or consumed offsets aren't 0, 1, 2, ... in order")

        # 2. End offsets via ListOffsets.
        c = KafkaConsumer(bootstrap_servers=b.addr, group_id=None)
        ends = c.end_offsets([TopicPartition("orders", i) for i in range(3)])
        begins = c.beginning_offsets([TopicPartition("orders", i) for i in range(3)])
        c.close()
        wrong = sum(1 for i in range(3) if ends[TopicPartition("orders", i)] != len([1 for m in acked if m.partition == i]))
        wrong += sum(1 for v in begins.values() if v != 0)
        table.add("list-offsets", wrong, 0, "partitions whose beginning/end offsets disagree with what was produced")

        # 3. Clean restart, then everything is still there.
        b.stop()
        b.start()
        got = consume_all(b.addr, "orders", 3, n)
        table.add("restart", n - len(set((p_, o) for p_, o, _, _ in got)), 0, "messages missing after a clean broker restart")

        # 4. kill -9 during a stream of acked produces: nothing acked may be lost.
        p = KafkaProducer(bootstrap_servers=b.addr, acks="all", enable_idempotence=False, linger_ms=0, retries=0)
        acked_kill = []
        for i in range(3000):
            acked_kill.append(p.send("kill", value=f"m{i}".encode(), partition=0).get(timeout=10))
        b.stop(kill=True)
        p.close(timeout=1)
        b.start()
        got = consume_all(b.addr, "kill", 1, len(acked_kill))
        table.add("kill-9", len(acked_kill) - len(got), 0, f"of {len(acked_kill)} acked messages, lost after kill -9 of the broker")

        # 5. Default producer settings (idempotence on) need InitProducerId, which isn't implemented.
        ok = 0
        try:
            p = KafkaProducer(bootstrap_servers=b.addr, max_block_ms=3000)
            p.send("idem", b"x").get(timeout=5)
            p.close(timeout=1)
            ok = 1
        except Exception:
            pass
        table.add("idempotent-producer", 1 - ok, 0, "kafka-python with default settings (enable_idempotence=True) can't produce", gap=True)
        b.stop()

        # 6. Torn writes: the storage crash harness.
        r = subprocess.run([binary, "crashcheck", "--trials", "200"], capture_output=True, text=True)
        failed = int(r.stdout.split()[0]) if r.stdout.split() else 200
        table.add("torn-tail", failed / 200, 0, "fraction of simulated power-loss cuts after which recovery loses or corrupts data", gap=True)
    finally:
        b.stop(kill=True)
        shutil.rmtree(tmp, ignore_errors=True)
    return 1 if table.print(time.time() - t0) else 0


if __name__ == "__main__":
    sys.exit(main())
