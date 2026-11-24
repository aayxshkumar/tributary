// Package record builds and parses Kafka record batches (magic v2), the
// on-disk and on-the-wire format. The broker only needs the batch header;
// the full parser is for tests, the crash harness and the dashboard.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/aayxshkumar/tributary/internal/wire"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Byte offsets inside a batch header.
const (
	OffBaseOffset      = 0
	OffBatchLength     = 8
	OffLeaderEpoch     = 12
	OffMagic           = 16
	OffCRC             = 17
	OffAttributes      = 21
	OffLastOffsetDelta = 23
	OffBaseTimestamp   = 27
	OffMaxTimestamp    = 35
	OffProducerID      = 43
	OffProducerEpoch   = 51
	OffBaseSequence    = 53
	OffRecordCount     = 57
	HeaderSize         = 61
)

// Header is the part of a batch the broker reads.
type Header struct {
	BaseOffset      int64
	Length          int // whole batch, including the 12-byte offset+length prefix
	LastOffsetDelta int32
	MaxTimestamp    int64
	RecordCount     int32
	Attributes      int16
}

var (
	ErrShort = errors.New("record: batch truncated")
	ErrMagic = errors.New("record: not a magic v2 batch")
	ErrCRC   = errors.New("record: CRC mismatch")
)

func CRC(batch []byte) uint32 { return crc32.Checksum(batch[OffAttributes:], castagnoli) }

// ParseHeader reads and checks a batch at the start of b (b may hold more
// after it). It verifies the length, the magic byte and the CRC.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrShort
	}
	n := int(int32(binary.BigEndian.Uint32(b[OffBatchLength:])))
	if n < HeaderSize-12 || 12+n > len(b) {
		return Header{}, ErrShort
	}
	batch := b[:12+n]
	if batch[OffMagic] != 2 {
		return Header{}, ErrMagic
	}
	if binary.BigEndian.Uint32(batch[OffCRC:]) != CRC(batch) {
		return Header{}, ErrCRC
	}
	return Header{
		BaseOffset:      int64(binary.BigEndian.Uint64(batch[OffBaseOffset:])),
		Length:          12 + n,
		LastOffsetDelta: int32(binary.BigEndian.Uint32(batch[OffLastOffsetDelta:])),
		MaxTimestamp:    int64(binary.BigEndian.Uint64(batch[OffMaxTimestamp:])),
		RecordCount:     int32(binary.BigEndian.Uint32(batch[OffRecordCount:])),
		Attributes:      int16(binary.BigEndian.Uint16(batch[OffAttributes:])),
	}, nil
}

// SetBaseOffset rewrites the base offset. The CRC starts at the attributes,
// so it stays valid.
func SetBaseOffset(batch []byte, base int64) {
	binary.BigEndian.PutUint64(batch[OffBaseOffset:], uint64(base))
}

type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte // nil = null key
	Value     []byte // nil = null value
}

// Build makes an uncompressed batch from records (offsets are relative:
// record i gets offset delta i). The base offset is 0 until the log assigns one.
func Build(recs []Record, baseTimestamp int64) []byte {
	var body wire.Writer
	maxTs := baseTimestamp
	for i, r := range recs {
		if r.Timestamp > maxTs {
			maxTs = r.Timestamp
		}
		var rec wire.Writer
		rec.Int8(0) // attributes
		rec.Varint(r.Timestamp - baseTimestamp)
		rec.Varint(int64(i))
		if r.Key == nil {
			rec.Varint(-1)
		} else {
			rec.Varint(int64(len(r.Key)))
			rec.Raw(r.Key)
		}
		if r.Value == nil {
			rec.Varint(-1)
		} else {
			rec.Varint(int64(len(r.Value)))
			rec.Raw(r.Value)
		}
		rec.Varint(0) // no headers
		body.Varint(int64(len(rec.B)))
		body.Raw(rec.B)
	}
	var w wire.Writer
	w.Int64(0)                      // base offset
	w.Int32(0)                      // batch length, filled below
	w.Int32(-1)                     // partition leader epoch
	w.Int8(2)                       // magic
	w.Int32(0)                      // CRC, filled below
	w.Int16(0)                      // attributes: no compression, create time
	w.Int32(int32(len(recs) - 1))   // last offset delta
	w.Int64(baseTimestamp)          // base timestamp
	w.Int64(maxTs)                  // max timestamp
	w.Int64(-1)                     // producer id
	w.Int16(-1)                     // producer epoch
	w.Int32(-1)                     // base sequence
	w.Int32(int32(len(recs)))       // record count
	w.Raw(body.B)
	b := w.B
	binary.BigEndian.PutUint32(b[OffBatchLength:], uint32(len(b)-12))
	binary.BigEndian.PutUint32(b[OffCRC:], CRC(b))
	return b
}

// Parse returns the records of one uncompressed batch with absolute offsets.
func Parse(batch []byte) ([]Record, error) {
	h, err := ParseHeader(batch)
	if err != nil {
		return nil, err
	}
	if h.Attributes&0x7 != 0 {
		return nil, fmt.Errorf("record: compressed batches (codec %d) are not parsed", h.Attributes&0x7)
	}
	baseTs := int64(binary.BigEndian.Uint64(batch[OffBaseTimestamp:]))
	r := wire.NewReader(batch[HeaderSize:h.Length])
	out := make([]Record, 0, h.RecordCount)
	for i := int32(0); i < h.RecordCount; i++ {
		n := r.Varint()
		rr := wire.NewReader(r.Raw(int(n)))
		rr.Int8()
		tsDelta := rr.Varint()
		offDelta := rr.Varint()
		var key, value []byte
		if kl := rr.Varint(); kl >= 0 {
			key = append([]byte{}, rr.Raw(int(kl))...)
		}
		if vl := rr.Varint(); vl >= 0 {
			value = append([]byte{}, rr.Raw(int(vl))...)
		}
		if rr.Err() != nil || r.Err() != nil {
			return nil, ErrShort
		}
		out = append(out, Record{Offset: h.BaseOffset + offDelta, Timestamp: baseTs + tsDelta, Key: key, Value: value})
	}
	return out, nil
}

// Split walks back-to-back batches (as in a fetch response or a segment).
// It stops at the first incomplete or invalid batch and returns what it had.
func Split(b []byte) ([][]byte, error) {
	var out [][]byte
	for len(b) > 0 {
		h, err := ParseHeader(b)
		if err != nil {
			return out, err
		}
		out = append(out, b[:h.Length])
		b = b[h.Length:]
	}
	return out, nil
}
