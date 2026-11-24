// Package storage is the partition log: record batches appended back to back
// in one segment file per partition, with an in-memory index of where each
// batch starts.
//
// KNOWN GAP (crash safety): nothing is fsynced, and recovery trusts the file.
// On open it scans batch headers until one doesn't fit in the file and stops
// there, but it neither checks CRCs nor truncates the torn tail. New batches
// are then appended after the garbage, so they are readable until the next
// restart and lost after it. tributary crashcheck measures this.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/aayxshkumar/tributary/internal/record"
)

var ErrOffsetOutOfRange = errors.New("storage: offset out of range")

type batchPos struct {
	base, last int64 // first and last offset in the batch
	pos        int64 // byte position in the segment
	size       int
	maxTs      int64
}

type Partition struct {
	mu      sync.RWMutex
	path    string
	f       *os.File
	batches []batchPos
	next    int64 // log end offset: the offset the next record gets
	size    int64 // bytes in the segment, as far as we know
}

// Open opens (or creates) the partition in dir and rebuilds the batch index.
func Open(dir string) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	p := &Partition{path: path, f: f}
	if err := p.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

func (p *Partition) recover() error {
	st, err := p.f.Stat()
	if err != nil {
		return err
	}
	fileSize := st.Size()
	var pos int64
	head := make([]byte, record.HeaderSize)
	for pos+int64(record.HeaderSize) <= fileSize {
		if _, err := p.f.ReadAt(head, pos); err != nil {
			return fmt.Errorf("storage: reading %s at %d: %w", p.path, pos, err)
		}
		n := int64(int32(binary.BigEndian.Uint32(head[record.OffBatchLength:])))
		if n <= 0 || pos+12+n > fileSize {
			break // torn tail: stop here (but the bytes stay in the file)
		}
		base := int64(binary.BigEndian.Uint64(head[record.OffBaseOffset:]))
		last := base + int64(int32(binary.BigEndian.Uint32(head[record.OffLastOffsetDelta:])))
		maxTs := int64(binary.BigEndian.Uint64(head[record.OffMaxTimestamp:]))
		p.batches = append(p.batches, batchPos{base: base, last: last, pos: pos, size: int(12 + n), maxTs: maxTs})
		p.next = last + 1
		pos += 12 + n
	}
	p.size = fileSize
	return nil
}

// Append validates one batch, gives it the next offsets and writes it.
// Returns the base offset assigned.
func (p *Partition) Append(batch []byte) (int64, error) {
	h, err := record.ParseHeader(batch)
	if err != nil {
		return 0, err
	}
	if h.Length != len(batch) {
		return 0, fmt.Errorf("storage: %d trailing bytes after the batch", len(batch)-h.Length)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	base := p.next
	b := append([]byte(nil), batch...)
	record.SetBaseOffset(b, base)
	if _, err := p.f.Write(b); err != nil {
		return 0, err
	}
	p.batches = append(p.batches, batchPos{base: base, last: base + int64(h.LastOffsetDelta), pos: p.size, size: len(b), maxTs: h.MaxTimestamp})
	p.size += int64(len(b))
	p.next = base + int64(h.LastOffsetDelta) + 1
	return base, nil
}

func (p *Partition) EndOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.next
}

func (p *Partition) StartOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.batches) == 0 {
		return p.next
	}
	return p.batches[0].base
}

func (p *Partition) Bytes() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.size
}

// Read returns whole batches starting with the one that holds offset, up to
// maxBytes (at least one batch, so a big batch can't stall a consumer).
func (p *Partition) Read(offset int64, maxBytes int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset == p.next {
		return nil, nil
	}
	if offset > p.next || len(p.batches) == 0 || offset < p.batches[0].base {
		return nil, ErrOffsetOutOfRange
	}
	i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].last >= offset })
	if i == len(p.batches) {
		return nil, ErrOffsetOutOfRange
	}
	start := p.batches[i].pos
	end := start
	for j := i; j < len(p.batches); j++ {
		if end-start > 0 && end-start+int64(p.batches[j].size) > int64(maxBytes) {
			break
		}
		end = p.batches[j].pos + int64(p.batches[j].size)
	}
	buf := make([]byte, end-start)
	if _, err := p.f.ReadAt(buf, start); err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

// OffsetForTime: the first offset of the first batch whose max timestamp is
// at or after ts (-1 when none).
func (p *Partition) OffsetForTime(ts int64) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, b := range p.batches {
		if b.maxTs >= ts {
			return b.base
		}
	}
	return -1
}

func (p *Partition) Close() error { return p.f.Close() }

// Path of the segment file (the crash harness truncates it).
func (p *Partition) Path() string { return p.path }
