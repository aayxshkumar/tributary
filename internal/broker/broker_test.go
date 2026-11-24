package broker

import (
	"testing"

	"github.com/aayxshkumar/tributary/internal/record"
	"github.com/aayxshkumar/tributary/internal/wire"
)

func header(w *wire.Writer, key, version int16, corr int32) {
	w.Int16(key)
	w.Int16(version)
	w.Int32(corr)
	w.String("test")
}

func newBroker(t *testing.T) *Broker {
	b, err := New(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

func TestApiVersionsTooNewGetsV0Error(t *testing.T) {
	b := newBroker(t)
	var w wire.Writer
	header(&w, KeyApiVersions, 4, 7)
	resp, action := b.Handle(w.B)
	r := wire.NewReader(resp)
	if action != Respond || r.Int32() != 7 || r.Int16() != ErrUnsupportedVersion {
		t.Fatal("expected UNSUPPORTED_VERSION in v0 format")
	}
	n := r.ArrayLen()
	found := false
	for i := 0; i < n; i++ {
		k, _, max := r.Int16(), r.Int16(), r.Int16()
		if k == KeyApiVersions && max == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("ApiVersions range missing from the list")
	}
}

func TestUnsupportedVersionClosesConnection(t *testing.T) {
	b := newBroker(t)
	var w wire.Writer
	header(&w, KeyFetch, 11, 1)
	if _, action := b.Handle(w.B); action != CloseConn {
		t.Fatal("expected the connection to be closed")
	}
}

func produce(t *testing.T, b *Broker, acks int16, batch []byte) ([]byte, int) {
	var w wire.Writer
	header(&w, KeyProduce, 3, 9)
	w.NullableString("", false)
	w.Int16(acks)
	w.Int32(1000)
	w.ArrayLen(1)
	w.String("t")
	w.ArrayLen(1)
	w.Int32(1)
	w.Bytes(batch)
	return b.Handle(w.B)
}

func TestProduceThenFetch(t *testing.T) {
	b := newBroker(t)
	batch := record.Build([]record.Record{{Value: []byte("a")}, {Value: []byte("b")}}, 1)
	for i := 0; i < 2; i++ {
		resp, action := produce(t, b, 1, batch)
		r := wire.NewReader(resp)
		r.Int32()
		r.ArrayLen()
		_ = r.String()
		r.ArrayLen()
		if action != Respond || r.Int32() != 1 || r.Int16() != ErrNone || r.Int64() != int64(2*i) {
			t.Fatalf("produce %d", i)
		}
	}
	var w wire.Writer
	header(&w, KeyFetch, 4, 3)
	w.Int32(-1)
	w.Int32(0)
	w.Int32(1)
	w.Int32(1 << 20)
	w.Int8(0)
	w.ArrayLen(1)
	w.String("t")
	w.ArrayLen(1)
	w.Int32(1)
	w.Int64(2)
	w.Int32(1 << 20)
	resp, _ := b.Handle(w.B)
	r := wire.NewReader(resp)
	r.Int32()
	r.Int32()
	r.ArrayLen()
	_ = r.String()
	r.ArrayLen()
	part, errCode, hw := r.Int32(), r.Int16(), r.Int64()
	r.Int64()
	r.ArrayLen()
	data := r.Bytes()
	recs, err := record.Parse(data)
	if part != 1 || errCode != ErrNone || hw != 4 || err != nil || len(recs) != 2 || recs[0].Offset != 2 {
		t.Fatalf("fetch: part %d err %d hw %d recs %v %v", part, errCode, hw, recs, err)
	}
}

func TestAcksZeroHasNoResponse(t *testing.T) {
	b := newBroker(t)
	if _, action := produce(t, b, 0, record.Build([]record.Record{{Value: []byte("x")}}, 0)); action != NoResponse {
		t.Fatal("acks=0 produce got a response")
	}
}

func TestCorruptBatchRejected(t *testing.T) {
	b := newBroker(t)
	batch := record.Build([]record.Record{{Value: []byte("x")}}, 0)
	batch[len(batch)-1] ^= 0xff
	resp, _ := produce(t, b, 1, batch)
	r := wire.NewReader(resp)
	r.Int32()
	r.ArrayLen()
	_ = r.String()
	r.ArrayLen()
	r.Int32()
	if r.Int16() != ErrCorruptMessage {
		t.Fatal("corrupt batch accepted")
	}
}
