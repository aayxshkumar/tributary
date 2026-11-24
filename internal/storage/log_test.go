package storage

import (
	"os"
	"testing"

	"github.com/aayxshkumar/tributary/internal/record"
)

func batch(vals ...string) []byte {
	var recs []record.Record
	for _, v := range vals {
		recs = append(recs, record.Record{Timestamp: 1700000000000, Key: []byte("k"), Value: []byte(v)})
	}
	return record.Build(recs, 1700000000000)
}

func TestAppendReopenRead(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if base, err := p.Append(batch("a", "b")); err != nil || base != int64(2*i) {
			t.Fatalf("append %d: base %d err %v", i, base, err)
		}
	}
	p.Close()
	p, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.EndOffset() != 6 {
		t.Fatalf("end offset %d after reopen, want 6", p.EndOffset())
	}
	data, err := p.Read(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := record.Split(data)
	if err != nil || len(batches) != 2 {
		t.Fatalf("read from 2: %d batches, err %v", len(batches), err)
	}
	recs, err := record.Parse(batches[0])
	if err != nil || recs[0].Offset != 2 || string(recs[1].Value) != "b" {
		t.Fatalf("records %+v err %v", recs, err)
	}
}

func TestReadPastEnd(t *testing.T) {
	p, _ := Open(t.TempDir())
	defer p.Close()
	p.Append(batch("x"))
	if d, err := p.Read(1, 100); err != nil || d != nil {
		t.Fatalf("read at end: %v %v", d, err)
	}
	if _, err := p.Read(5, 100); err != ErrOffsetOutOfRange {
		t.Fatalf("read past end: %v", err)
	}
}

func TestRejectsBadCRC(t *testing.T) {
	p, _ := Open(t.TempDir())
	defer p.Close()
	b := batch("x")
	b[len(b)-1] ^= 1
	if _, err := p.Append(b); err != record.ErrCRC {
		t.Fatalf("bad CRC accepted: %v", err)
	}
	_ = os.Remove
}
