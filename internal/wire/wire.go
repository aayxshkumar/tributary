// Package wire reads and writes the Kafka protocol's primitive types
// (big-endian integers, int16-prefixed strings, int32-prefixed bytes and
// arrays, zigzag varints) for the non-flexible API versions tributary speaks.
package wire

import (
	"encoding/binary"
	"errors"
)

var ErrShort = errors.New("wire: message too short")

// Reader decodes from a byte slice. The first error sticks; later reads
// return zero values, so handlers can read a whole request and check Err once.
type Reader struct {
	b   []byte
	pos int
	err error
}

func NewReader(b []byte) *Reader { return &Reader{b: b} }

func (r *Reader) Err() error     { return r.err }
func (r *Reader) Remaining() int { return len(r.b) - r.pos }

func (r *Reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.b) {
		r.err = ErrShort
		return nil
	}
	out := r.b[r.pos : r.pos+n]
	r.pos += n
	return out
}

func (r *Reader) Int8() int8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return int8(b[0])
}

func (r *Reader) Int16() int16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}

func (r *Reader) Int32() int32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

func (r *Reader) Int64() int64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// String reads an int16-length string; a length of -1 (null) returns "".
func (r *Reader) String() string {
	s, _ := r.NullableString()
	return s
}

func (r *Reader) NullableString() (string, bool) {
	n := r.Int16()
	if n < 0 {
		return "", false
	}
	return string(r.take(int(n))), true
}

// Bytes reads int32-length bytes; null returns nil.
func (r *Reader) Bytes() []byte {
	n := r.Int32()
	if n < 0 {
		return nil
	}
	return r.take(int(n))
}

// ArrayLen reads an int32 array length; null counts as empty. Lengths larger
// than the bytes left are rejected, so a bad length can't make us allocate.
func (r *Reader) ArrayLen() int {
	n := r.Int32()
	if n < 0 {
		return 0
	}
	if int(n) > r.Remaining() {
		r.err = ErrShort
		return 0
	}
	return int(n)
}

// Varint reads a zigzag-encoded signed varint (records use these).
func (r *Reader) Varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b[r.pos:])
	if n <= 0 {
		r.err = ErrShort
		return 0
	}
	r.pos += n
	return v
}

func (r *Reader) Raw(n int) []byte { return r.take(n) }

// Writer appends to a byte slice.
type Writer struct{ B []byte }

func (w *Writer) Int8(v int8)   { w.B = append(w.B, byte(v)) }
func (w *Writer) Int16(v int16) { w.B = binary.BigEndian.AppendUint16(w.B, uint16(v)) }
func (w *Writer) Int32(v int32) { w.B = binary.BigEndian.AppendUint32(w.B, uint32(v)) }
func (w *Writer) Int64(v int64) { w.B = binary.BigEndian.AppendUint64(w.B, uint64(v)) }
func (w *Writer) Bool(v bool) {
	if v {
		w.Int8(1)
	} else {
		w.Int8(0)
	}
}

func (w *Writer) String(s string) {
	w.Int16(int16(len(s)))
	w.B = append(w.B, s...)
}

func (w *Writer) NullableString(s string, ok bool) {
	if !ok {
		w.Int16(-1)
		return
	}
	w.String(s)
}

func (w *Writer) Bytes(b []byte) {
	if b == nil {
		w.Int32(-1)
		return
	}
	w.Int32(int32(len(b)))
	w.B = append(w.B, b...)
}

func (w *Writer) ArrayLen(n int) { w.Int32(int32(n)) }
func (w *Writer) Varint(v int64) { w.B = binary.AppendVarint(w.B, v) }
func (w *Writer) Raw(b []byte)   { w.B = append(w.B, b...) }
