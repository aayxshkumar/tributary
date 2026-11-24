// Package broker speaks the Kafka wire protocol over TCP: ApiVersions,
// Metadata, Produce, Fetch and ListOffsets, at the (non-flexible) versions
// listed in Supported. One broker, no replication, no consumer groups, no
// transactions or idempotent producers yet.
package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aayxshkumar/tributary/internal/record"
	"github.com/aayxshkumar/tributary/internal/storage"
	"github.com/aayxshkumar/tributary/internal/wire"
)

// Kafka API keys.
const (
	KeyProduce     int16 = 0
	KeyFetch       int16 = 1
	KeyListOffsets int16 = 2
	KeyMetadata    int16 = 3
	KeyApiVersions int16 = 18
)

// Kafka error codes.
const (
	ErrNone                    int16 = 0
	ErrOffsetOutOfRange        int16 = 1
	ErrCorruptMessage          int16 = 2
	ErrUnknownTopicOrPartition int16 = 3
	ErrUnsupportedVersion      int16 = 35
	ErrInvalidRequest          int16 = 42
)

type VersionRange struct{ Min, Max int16 }

// Supported is what ApiVersions advertises. Clients pick the highest version
// both sides know.
var Supported = map[int16]VersionRange{
	KeyProduce:     {3, 3},
	KeyFetch:       {4, 4},
	KeyListOffsets: {1, 1},
	KeyMetadata:    {1, 4}, // v4 makes clients infer a 0.11+ broker and send magic v2 batches
	KeyApiVersions: {0, 2},
}

type Topic struct {
	Name       string
	Partitions []*storage.Partition
}

type Stats struct {
	Requests       atomic.Int64
	ProducedMsgs   atomic.Int64
	ProducedBytes  atomic.Int64
	FetchedBytes   atomic.Int64
	Connections    atomic.Int64
	ProtocolErrors atomic.Int64
}

type Broker struct {
	NodeID            int32
	Host              string
	Port              int32
	DataDir           string
	DefaultPartitions int

	mu     sync.RWMutex
	topics map[string]*Topic
	Stats  Stats
	start  time.Time
}

func New(dataDir string, defaultPartitions int) (*Broker, error) {
	b := &Broker{NodeID: 1, Host: "127.0.0.1", DataDir: dataDir, DefaultPartitions: defaultPartitions,
		topics: map[string]*Topic{}, start: time.Now()}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	// Reopen existing partitions: directories named <topic>-<partition>.
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	parts := map[string][]int{}
	for _, e := range entries {
		i := strings.LastIndexByte(e.Name(), '-')
		if !e.IsDir() || i <= 0 {
			continue
		}
		n, err := strconv.Atoi(e.Name()[i+1:])
		if err != nil {
			continue
		}
		parts[e.Name()[:i]] = append(parts[e.Name()[:i]], n)
	}
	for name, ns := range parts {
		sort.Ints(ns)
		t := &Topic{Name: name}
		for k := range ns {
			p, err := storage.Open(filepath.Join(dataDir, fmt.Sprintf("%s-%d", name, k)))
			if err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		b.topics[name] = t
	}
	return b, nil
}

func validTopicName(s string) bool {
	if s == "" || len(s) > 249 || s == "." || s == ".." {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// TopicFor returns the topic, creating it with the default partition count
// when create is true (auto-create on metadata, like Kafka's default).
func (b *Broker) TopicFor(name string, create bool) (*Topic, error) {
	b.mu.RLock()
	t := b.topics[name]
	b.mu.RUnlock()
	if t != nil || !create {
		return t, nil
	}
	if !validTopicName(name) {
		return nil, fmt.Errorf("invalid topic name %q", name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t := b.topics[name]; t != nil {
		return t, nil
	}
	t = &Topic{Name: name}
	for k := 0; k < b.DefaultPartitions; k++ {
		p, err := storage.Open(filepath.Join(b.DataDir, fmt.Sprintf("%s-%d", name, k)))
		if err != nil {
			return nil, err
		}
		t.Partitions = append(t.Partitions, p)
	}
	b.topics[name] = t
	return t, nil
}

func (b *Broker) partition(topic string, idx int32) *storage.Partition {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t := b.topics[topic]
	if t == nil || idx < 0 || int(idx) >= len(t.Partitions) {
		return nil
	}
	return t.Partitions[idx]
}

func (b *Broker) TopicNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for n := range b.topics {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (b *Broker) Topic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

func (b *Broker) Uptime() time.Duration { return time.Since(b.start) }

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			p.Close()
		}
	}
}

// ------------------------------------------------------------------ server

// Serve accepts connections until the listener is closed.
func (b *Broker) Serve(ln net.Listener) error {
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		b.Port = int32(addr.Port)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go b.handleConn(c)
	}
}

func (b *Broker) handleConn(c net.Conn) {
	b.Stats.Connections.Add(1)
	defer b.Stats.Connections.Add(-1)
	defer c.Close()
	var sizeBuf [4]byte
	for {
		if _, err := io.ReadFull(c, sizeBuf[:]); err != nil {
			return
		}
		n := int32(binary.BigEndian.Uint32(sizeBuf[:]))
		if n < 8 || n > 100<<20 {
			b.Stats.ProtocolErrors.Add(1)
			return
		}
		req := make([]byte, n)
		if _, err := io.ReadFull(c, req); err != nil {
			return
		}
		resp, action := b.Handle(req)
		if action == NoResponse {
			continue // acks=0 produce: no response at all
		}
		if action == CloseConn {
			return // like Kafka: an API or version we don't speak closes the connection
		}
		out := make([]byte, 4, 4+len(resp))
		binary.BigEndian.PutUint32(out, uint32(len(resp)))
		out = append(out, resp...)
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}

// What to do after a request.
const (
	Respond    = 0
	NoResponse = 1
	CloseConn  = 2
)

// Handle one request (without the size prefix). Returns the response
// (correlation id first, without the size prefix) and what to do with it.
func (b *Broker) Handle(req []byte) ([]byte, int) {
	b.Stats.Requests.Add(1)
	r := wire.NewReader(req)
	key, version, corr := r.Int16(), r.Int16(), r.Int32()
	var w wire.Writer
	w.Int32(corr)
	vr, known := Supported[key]
	if key == KeyApiVersions && (!known || version > vr.Max) {
		// Answer in the v0 format with UNSUPPORTED_VERSION and our list, as
		// Kafka does, so the client retries with a version we speak.
		writeApiVersions(&w, ErrUnsupportedVersion, 0)
		return w.B, Respond
	}
	if !known || version < vr.Min || version > vr.Max {
		b.Stats.ProtocolErrors.Add(1)
		log.Printf("tributary: unsupported api %d v%d, closing the connection", key, version)
		return nil, CloseConn
	}
	r.NullableString() // client id (request header v1)
	if r.Err() != nil {
		b.Stats.ProtocolErrors.Add(1)
		return nil, CloseConn
	}
	switch key {
	case KeyApiVersions:
		writeApiVersions(&w, ErrNone, version)
	case KeyMetadata:
		b.metadata(r, &w, version)
	case KeyProduce:
		if !b.produce(r, &w) {
			return nil, NoResponse
		}
	case KeyFetch:
		b.fetch(r, &w)
	case KeyListOffsets:
		b.listOffsets(r, &w)
	}
	return w.B, Respond
}

func writeApiVersions(w *wire.Writer, errCode int16, version int16) {
	w.Int16(errCode)
	keys := make([]int, 0, len(Supported))
	for k := range Supported {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	w.ArrayLen(len(keys))
	for _, k := range keys {
		v := Supported[int16(k)]
		w.Int16(int16(k))
		w.Int16(v.Min)
		w.Int16(v.Max)
	}
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
}

// Metadata v1-v4.
func (b *Broker) metadata(r *wire.Reader, w *wire.Writer, version int16) {
	n := r.Int32()
	var names []string
	all := n < 0
	for i := int32(0); i < n && r.Err() == nil; i++ {
		names = append(names, r.String())
	}
	autoCreate := true
	if version >= 4 {
		autoCreate = r.Int8() != 0
	}
	if all {
		names = b.TopicNames()
	}
	if version >= 3 {
		w.Int32(0) // throttle
	}
	w.ArrayLen(1) // brokers
	w.Int32(b.NodeID)
	w.String(b.Host)
	w.Int32(b.Port)
	w.NullableString("", false) // rack
	if version >= 2 {
		w.String("tributary") // cluster id
	}
	w.Int32(b.NodeID) // controller
	w.ArrayLen(len(names))
	for _, name := range names {
		t, err := b.TopicFor(name, autoCreate)
		if err != nil || t == nil {
			w.Int16(ErrUnknownTopicOrPartition)
			w.String(name)
			w.Bool(false)
			w.ArrayLen(0)
			continue
		}
		w.Int16(ErrNone)
		w.String(name)
		w.Bool(false)
		w.ArrayLen(len(t.Partitions))
		for i := range t.Partitions {
			w.Int16(ErrNone)
			w.Int32(int32(i))
			w.Int32(b.NodeID) // leader
			w.ArrayLen(1)
			w.Int32(b.NodeID) // replicas
			w.ArrayLen(1)
			w.Int32(b.NodeID) // in-sync replicas
		}
	}
}

// Produce v3. Returns false for acks=0 (no response).
func (b *Broker) produce(r *wire.Reader, w *wire.Writer) bool {
	r.NullableString() // transactional id
	acks := r.Int16()
	r.Int32() // timeout
	type result struct {
		part int32
		err  int16
		base int64
	}
	type topicResult struct {
		name  string
		parts []result
	}
	var out []topicResult
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		tr := topicResult{name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			idx := r.Int32()
			records := r.Bytes()
			res := result{part: idx, base: -1}
			p := b.partition(tr.name, idx)
			if p == nil {
				if _, err := b.TopicFor(tr.name, true); err == nil {
					p = b.partition(tr.name, idx)
				}
			}
			if p == nil {
				res.err = ErrUnknownTopicOrPartition
			} else {
				batches, err := record.Split(records)
				if err != nil || len(batches) == 0 {
					res.err = ErrCorruptMessage
				}
				for k, batch := range batches {
					base, err := p.Append(batch)
					if err != nil {
						res.err = ErrCorruptMessage
						break
					}
					if k == 0 {
						res.base = base
					}
					h, _ := record.ParseHeader(batch)
					b.Stats.ProducedMsgs.Add(int64(h.RecordCount))
					b.Stats.ProducedBytes.Add(int64(len(batch)))
				}
			}
			tr.parts = append(tr.parts, res)
		}
		out = append(out, tr)
	}
	if acks == 0 {
		return false
	}
	w.ArrayLen(len(out))
	for _, t := range out {
		w.String(t.name)
		w.ArrayLen(len(t.parts))
		for _, p := range t.parts {
			w.Int32(p.part)
			w.Int16(p.err)
			w.Int64(p.base)
			w.Int64(-1) // log append time: not used (create time)
		}
	}
	w.Int32(0) // throttle
	return true
}

// Fetch v4.
func (b *Broker) fetch(r *wire.Reader, w *wire.Writer) {
	r.Int32() // replica id
	maxWait := time.Duration(r.Int32()) * time.Millisecond
	minBytes := int(r.Int32())
	maxBytes := int(r.Int32())
	r.Int8() // isolation level: no transactions, so it doesn't matter yet
	type req struct {
		topic    string
		part     int32
		offset   int64
		maxBytes int
	}
	var reqs []req
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		name := r.String()
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			reqs = append(reqs, req{topic: name, part: r.Int32(), offset: r.Int64(), maxBytes: int(r.Int32())})
		}
	}
	if maxBytes <= 0 {
		maxBytes = 50 << 20
	}
	type res struct {
		err  int16
		hw   int64
		data []byte
	}
	collect := func() ([]res, int) {
		out := make([]res, len(reqs))
		total := 0
		for i, q := range reqs {
			p := b.partition(q.topic, q.part)
			if p == nil {
				out[i] = res{err: ErrUnknownTopicOrPartition, hw: -1}
				continue
			}
			budget := q.maxBytes
			if left := maxBytes - total; left < budget {
				budget = left
			}
			data, err := p.Read(q.offset, budget)
			switch {
			case errors.Is(err, storage.ErrOffsetOutOfRange):
				out[i] = res{err: ErrOffsetOutOfRange, hw: p.EndOffset()}
			case err != nil:
				out[i] = res{err: ErrCorruptMessage, hw: p.EndOffset()}
			default:
				out[i] = res{hw: p.EndOffset(), data: data}
				total += len(data)
			}
		}
		return out, total
	}
	results, total := collect()
	deadline := time.Now().Add(maxWait)
	for total < minBytes && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		results, total = collect()
	}
	b.Stats.FetchedBytes.Add(int64(total))
	w.Int32(0) // throttle
	// Group back by topic, keeping request order.
	type group struct {
		name string
		idx  []int
	}
	var groups []group
	for i, q := range reqs {
		if len(groups) == 0 || groups[len(groups)-1].name != q.topic {
			groups = append(groups, group{name: q.topic})
		}
		groups[len(groups)-1].idx = append(groups[len(groups)-1].idx, i)
	}
	w.ArrayLen(len(groups))
	for _, g := range groups {
		w.String(g.name)
		w.ArrayLen(len(g.idx))
		for _, i := range g.idx {
			w.Int32(reqs[i].part)
			w.Int16(results[i].err)
			w.Int64(results[i].hw)
			w.Int64(results[i].hw) // last stable offset = high watermark (no transactions)
			w.ArrayLen(-1)         // aborted transactions: null
			if results[i].data == nil {
				w.Bytes([]byte{})
			} else {
				w.Bytes(results[i].data)
			}
		}
	}
}

// ListOffsets v1.
func (b *Broker) listOffsets(r *wire.Reader, w *wire.Writer) {
	r.Int32() // replica id
	type q struct {
		part int32
		ts   int64
	}
	type t struct {
		name  string
		parts []q
	}
	var ts []t
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		tt := t{name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			tt.parts = append(tt.parts, q{part: r.Int32(), ts: r.Int64()})
		}
		ts = append(ts, tt)
	}
	w.ArrayLen(len(ts))
	for _, tt := range ts {
		w.String(tt.name)
		w.ArrayLen(len(tt.parts))
		for _, pq := range tt.parts {
			w.Int32(pq.part)
			p := b.partition(tt.name, pq.part)
			if p == nil {
				w.Int16(ErrUnknownTopicOrPartition)
				w.Int64(-1)
				w.Int64(-1)
				continue
			}
			var off int64
			switch pq.ts {
			case -1:
				off = p.EndOffset()
			case -2:
				off = p.StartOffset()
			default:
				off = p.OffsetForTime(pq.ts)
			}
			w.Int16(ErrNone)
			w.Int64(-1)
			w.Int64(off)
		}
	}
}
