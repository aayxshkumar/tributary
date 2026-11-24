"""Plant known bugs in tributary one at a time and report what catches each.

For every mutant: rewrite one file in a scratch copy, build, run `go test
./...` and tools/check.py (real kafka-python client + crash harness).

    python3 tools/mutants.py [--only NAME]
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MUTANTS = [
    ("crc-not-checked", "internal/record/record.go", "if binary.BigEndian.Uint32(batch[OffCRC:]) != CRC(batch) {",
     "if false && binary.BigEndian.Uint32(batch[OffCRC:]) != CRC(batch) {", "batch CRCs are never verified"),
    ("next-offset-off-by-one", "internal/storage/log.go", "p.next = base + int64(h.LastOffsetDelta) + 1",
     "p.next = base + int64(h.LastOffsetDelta)", "the next batch reuses the last offset of the previous one"),
    ("read-skips-first-batch", "internal/storage/log.go",
     "i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].last >= offset })",
     "i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].last > offset })",
     "a fetch from the last offset of a batch skips that batch"),
    ("recover-drops-last", "internal/storage/log.go", "if n <= 0 || pos+12+n > fileSize {",
     "if n <= 0 || pos+12+n >= fileSize {", "recovery drops a complete final batch"),
    ("fetch-hw-stale", "internal/broker/broker.go", "out[i] = res{hw: p.EndOffset(), data: data}",
     "out[i] = res{hw: p.EndOffset() - 1, data: data}", "fetch reports a high watermark one behind"),
    ("list-latest-is-earliest", "internal/broker/broker.go", "\t\t\tcase -1:\n\t\t\t\toff = p.EndOffset()",
     "\t\t\tcase -1:\n\t\t\t\toff = p.StartOffset()", "ListOffsets latest returns the start of the log"),
    ("produce-base-wrong", "internal/broker/broker.go", "if k == 0 {\n\t\t\t\t\t\tres.base = base",
     "if k == 0 {\n\t\t\t\t\t\tres.base = base + 1", "Produce acks report the wrong base offset"),
    ("acks0-responds", "internal/broker/broker.go", "if acks == 0 {\n\t\treturn false", "if acks == -100 {\n\t\treturn false",
     "acks=0 produce gets a response the client never reads"),
    ("metadata-v2-no-cluster", "internal/broker/broker.go", "if version >= 2 {\n\t\tw.String(\"tributary\")",
     "if version >= 5 {\n\t\tw.String(\"tributary\")", "Metadata v2+ responses miss the cluster id"),
    ("varint-unsigned", "internal/wire/wire.go", "v, n := binary.Varint(r.b[r.pos:])", "u, n := binary.Uvarint(r.b[r.pos:]); v := int64(u)",
     "record varints read without zigzag decoding"),
    ("min-bytes-ignored", "internal/broker/broker.go", "for total < minBytes && time.Now().Before(deadline) {",
     "for false && total < minBytes && time.Now().Before(deadline) {", "fetch returns at once instead of waiting for min_bytes"),
]


def run(cmd, cwd, env=None):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=900, env=env)
    return p.returncode, p.stdout + p.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only")
    args = ap.parse_args()
    todo = [m for m in MUTANTS if not args.only or m[0] == args.only]
    tmp = tempfile.mkdtemp(prefix="tributary-mutants-")
    src = os.path.join(tmp, "tributary")
    shutil.copytree(ROOT, src, ignore=shutil.ignore_patterns(".git", "data", "__pycache__"))
    if os.path.isfile(os.path.join(src, "tributary")):
        os.remove(os.path.join(src, "tributary"))  # the built binary, not the cmd/tributary package
    binary = os.path.join(tmp, "bin", "tributary")
    print(f"{'mutant':<26}{'tests':>8}{'checks':>9}   meaning")
    survived = []
    for name, rel, old, new, meaning in todo:
        path = os.path.join(src, rel)
        original = open(path).read()
        if original.count(old) != 1:
            print(f"{name:<26}   pattern found {original.count(old)} times in {rel}, expected 1")
            survived.append(name)
            continue
        open(path, "w").write(original.replace(old, new))
        caught = []
        bcode, bout = run(["go", "build", "-o", binary, "./cmd/tributary"], src)
        if bcode:
            # A mutant that doesn't compile tests nothing: report it as broken, not caught.
            print(f"{name:<26}{'nobuild':>8}{'-':>9}   BROKEN MUTANT  {bout.strip().splitlines()[-1][:70]}")
            survived.append(name)
            open(path, "w").write(original)
            continue
        else:
            tcode, tout = run(["go", "test", "./..."], src)
            failed = sorted(set(l.split()[2] for l in tout.splitlines() if l.startswith("--- FAIL:")))
            ccode, cout = run([sys.executable, os.path.join(src, "tools", "check.py"), "--binary", binary], src)
            fails = [l.split()[0] for l in cout.splitlines() if " FAIL " in f" {l} " and not l.startswith("check")]
            tests = "fail" if tcode else "pass"
            checks = f"{len(fails)} fail" if ccode else "pass"
            if tcode:
                caught.append("tests: " + (", ".join(failed) or "failed"))
            if ccode:
                caught.append("checks: " + (", ".join(fails) or "crashed"))
        print(f"{name:<26}{tests:>8}{checks:>9}   {'' if caught else 'SURVIVED  '}{meaning}")
        for c in caught:
            print(f"{'':<31}{c}")
        if not caught:
            survived.append(name)
        open(path, "w").write(original)
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{len(todo) - len(survived)}/{len(todo)} caught" + (f"; survived: {', '.join(survived)}" if survived else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
