package wire

import "testing"

func TestRoundTrip(t *testing.T) {
	var w Writer
	w.Int8(-3)
	w.Int16(-300)
	w.Int32(70000)
	w.Int64(-1 << 40)
	w.String("topic")
	w.NullableString("", false)
	w.Bytes([]byte{1, 2, 3})
	w.Bytes(nil)
	w.ArrayLen(2)
	w.Varint(-129)
	r := NewReader(w.B)
	if r.Int8() != -3 || r.Int16() != -300 || r.Int32() != 70000 || r.Int64() != -1<<40 || r.String() != "topic" {
		t.Fatal("ints or string")
	}
	if _, ok := r.NullableString(); ok {
		t.Fatal("null string read as present")
	}
	if b := r.Bytes(); len(b) != 3 || b[2] != 3 {
		t.Fatal("bytes")
	}
	if r.Bytes() != nil {
		t.Fatal("null bytes")
	}
	if r.Int32() != 2 || r.Varint() != -129 || r.Err() != nil || r.Remaining() != 0 {
		t.Fatal("tail")
	}
}

func TestShortInputSticks(t *testing.T) {
	r := NewReader([]byte{0, 1})
	r.Int32()
	if r.Err() != ErrShort || r.Int16() != 0 {
		t.Fatal("expected sticky ErrShort")
	}
}

func TestHugeArrayLengthRejected(t *testing.T) {
	r := NewReader([]byte{0x7f, 0xff, 0xff, 0xff})
	if r.ArrayLen() != 0 || r.Err() == nil {
		t.Fatal("array length larger than the message accepted")
	}
}
