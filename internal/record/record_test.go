package record

import (
	"hash/crc32"
	"testing"
)

func TestCRC32CKnownValue(t *testing.T) {
	// CRC-32C of "123456789" is 0xE3069283.
	if got := crc32.Checksum([]byte("123456789"), castagnoli); got != 0xE3069283 {
		t.Fatalf("crc32c = %08x", got)
	}
}

func TestBuildParseRoundTrip(t *testing.T) {
	recs := []Record{
		{Timestamp: 1700000000000, Key: []byte("a"), Value: []byte("one")},
		{Timestamp: 1700000000005, Key: nil, Value: []byte("two")},
		{Timestamp: 1700000000009, Key: []byte("c"), Value: nil},
	}
	b := Build(recs, 1700000000000)
	SetBaseOffset(b, 40)
	h, err := ParseHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.BaseOffset != 40 || h.LastOffsetDelta != 2 || h.RecordCount != 3 || h.MaxTimestamp != 1700000000009 || h.Length != len(b) {
		t.Fatalf("header %+v", h)
	}
	got, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range got {
		if r.Offset != int64(40+i) || r.Timestamp != recs[i].Timestamp || string(r.Value) != string(recs[i].Value) ||
			(r.Key == nil) != (recs[i].Key == nil) {
			t.Fatalf("record %d: %+v", i, r)
		}
	}
}

func TestParseHeaderRejects(t *testing.T) {
	b := Build([]Record{{Value: []byte("x")}}, 0)
	if _, err := ParseHeader(b[:len(b)-1]); err != ErrShort {
		t.Fatalf("truncated: %v", err)
	}
	bad := append([]byte{}, b...)
	bad[len(bad)-1] ^= 1
	if _, err := ParseHeader(bad); err != ErrCRC {
		t.Fatalf("crc: %v", err)
	}
	bad = append([]byte{}, b...)
	bad[OffMagic] = 1
	if _, err := ParseHeader(bad); err != ErrMagic {
		t.Fatalf("magic: %v", err)
	}
}

func TestSplitStopsAtPartialBatch(t *testing.T) {
	a := Build([]Record{{Value: []byte("a")}}, 0)
	b := Build([]Record{{Value: []byte("b")}}, 0)
	all := append(append([]byte{}, a...), b[:len(b)-3]...)
	got, err := Split(all)
	if len(got) != 1 || err == nil {
		t.Fatalf("split: %d batches, err %v", len(got), err)
	}
}
