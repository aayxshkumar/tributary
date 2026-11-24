// tributary: a Kafka-protocol broker in Go (standard library only).
//
//	tributary serve [--port 9092] [--dashboard 8093] [--data data] [--partitions 3]
//	tributary crashcheck [--trials 200] [--seed 1]
//	tributary bench [--seconds 5] [--batch 100] [--size 100]
package main

import (
	"embed"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aayxshkumar/tributary/internal/broker"
	"github.com/aayxshkumar/tributary/internal/record"
	"github.com/aayxshkumar/tributary/internal/storage"
	"github.com/aayxshkumar/tributary/internal/wire"
)

//go:embed dashboard.html
var dashboard embed.FS

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tributary serve|crashcheck|bench [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "crashcheck":
		os.Exit(crashcheck(os.Args[2:]))
	case "bench":
		bench(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "tributary: unknown command %s\n", os.Args[1])
		os.Exit(2)
	}
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.Int("port", 9092, "Kafka protocol port")
	dash := fs.Int("dashboard", 8093, "dashboard port (0 = off)")
	data := fs.String("data", "data", "data directory")
	parts := fs.Int("partitions", 3, "partitions for auto-created topics")
	fs.Parse(args)
	b, err := broker.New(*data, *parts)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatal(err)
	}
	if *dash > 0 {
		go serveDashboard(b, *dash)
		log.Printf("tributary: dashboard on http://127.0.0.1:%d/", *dash)
	}
	log.Printf("tributary: Kafka protocol on %s, data in %s", ln.Addr(), *data)
	log.Fatal(b.Serve(ln))
}

func serveDashboard(b *broker.Broker, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		type part struct {
			Partition int   `json:"partition"`
			Start     int64 `json:"start"`
			End       int64 `json:"end"`
			Bytes     int64 `json:"bytes"`
		}
		type topic struct {
			Name       string `json:"name"`
			Partitions []part `json:"partitions"`
		}
		out := struct {
			UptimeS        float64 `json:"uptime_s"`
			Requests       int64   `json:"requests"`
			ProducedMsgs   int64   `json:"produced_msgs"`
			ProducedBytes  int64   `json:"produced_bytes"`
			FetchedBytes   int64   `json:"fetched_bytes"`
			Connections    int64   `json:"connections"`
			ProtocolErrors int64   `json:"protocol_errors"`
			Topics         []topic `json:"topics"`
		}{
			UptimeS: b.Uptime().Seconds(), Requests: b.Stats.Requests.Load(), ProducedMsgs: b.Stats.ProducedMsgs.Load(),
			ProducedBytes: b.Stats.ProducedBytes.Load(), FetchedBytes: b.Stats.FetchedBytes.Load(),
			Connections: b.Stats.Connections.Load(), ProtocolErrors: b.Stats.ProtocolErrors.Load(),
		}
		for _, name := range b.TopicNames() {
			t := b.Topic(name)
			tt := topic{Name: name}
			for i, p := range t.Partitions {
				tt.Partitions = append(tt.Partitions, part{Partition: i, Start: p.StartOffset(), End: p.EndOffset(), Bytes: p.Bytes()})
			}
			out.Topics = append(out.Topics, tt)
		}
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		page, _ := dashboard.ReadFile("dashboard.html")
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Write(page)
	})
	log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), mux))
}

// ------------------------------------------------------------------ crash harness

// crashcheck simulates power loss in the middle of a write: append batches,
// cut the segment file at a random byte (a torn tail), reopen, append more,
// reopen again, and check that every batch that was whole before the cut and
// every batch appended after recovery reads back intact and in order.
func crashcheck(args []string) int {
	fs := flag.NewFlagSet("crashcheck", flag.ExitOnError)
	trials := fs.Int("trials", 200, "trials")
	seed := fs.Int64("seed", 1, "seed")
	verbose := fs.Bool("v", false, "print each failing trial")
	fs.Parse(args)
	rng := rand.New(rand.NewSource(*seed))
	failed := 0
	for t := 0; t < *trials; t++ {
		dir, _ := os.MkdirTemp("", "tributary-crash-")
		why := crashTrial(dir, rng)
		os.RemoveAll(dir)
		if why != "" {
			failed++
			if *verbose {
				fmt.Printf("trial %d: %s\n", t, why)
			}
		}
	}
	fmt.Printf("%d of %d crash trials lost or corrupted data\n", failed, *trials)
	if failed > 0 {
		return 1
	}
	return 0
}

func randomBatch(rng *rand.Rand, tag string) ([]byte, []record.Record) {
	n := 1 + rng.Intn(20)
	recs := make([]record.Record, n)
	for i := range recs {
		recs[i] = record.Record{Timestamp: 1700000000000 + int64(i), Key: []byte(fmt.Sprintf("%s-k%d", tag, i)),
			Value: []byte(fmt.Sprintf("%s-v%d-%s", tag, i, string(make([]byte, rng.Intn(200)))))}
	}
	return record.Build(recs, 1700000000000), recs
}

func readAll(p *storage.Partition) ([]record.Record, error) {
	var out []record.Record
	for off := int64(0); off < p.EndOffset(); {
		data, err := p.Read(off, 1<<20)
		if err != nil {
			return out, err
		}
		batches, err := record.Split(data)
		if err != nil {
			return out, err
		}
		for _, b := range batches {
			recs, err := record.Parse(b)
			if err != nil {
				return out, err
			}
			out = append(out, recs...)
			off = recs[len(recs)-1].Offset + 1
		}
	}
	return out, nil
}

func crashTrial(dir string, rng *rand.Rand) string {
	p, err := storage.Open(dir)
	if err != nil {
		return err.Error()
	}
	var expect []record.Record
	var ends []int64 // file size after each batch
	n := 2 + rng.Intn(10)
	var keep [][]record.Record
	for i := 0; i < n; i++ {
		b, recs := randomBatch(rng, fmt.Sprintf("a%d", i))
		if _, err := p.Append(b); err != nil {
			return err.Error()
		}
		keep = append(keep, recs)
		ends = append(ends, p.Bytes())
	}
	path := p.Path()
	p.Close()
	// Cut somewhere inside the last few batches.
	size := ends[len(ends)-1]
	lo := int64(0)
	if len(ends) > 3 {
		lo = ends[len(ends)-4]
	}
	cut := lo + rng.Int63n(size-lo+1)
	if rng.Float64() < 0.25 {
		cut = ends[rng.Intn(len(ends))] // a clean cut between batches: should always survive
	}
	if err := os.Truncate(path, cut); err != nil {
		return err.Error()
	}
	for i, e := range ends {
		if e <= cut {
			expect = append(expect, keep[i]...)
		}
	}
	p, err = storage.Open(dir)
	if err != nil {
		return "reopen after cut: " + err.Error()
	}
	for i := 0; i < 1+rng.Intn(5); i++ {
		b, recs := randomBatch(rng, fmt.Sprintf("b%d", i))
		if _, err := p.Append(b); err != nil {
			return err.Error()
		}
		expect = append(expect, recs...)
	}
	p.Close()
	p, err = storage.Open(dir)
	if err != nil {
		return "second reopen: " + err.Error()
	}
	defer p.Close()
	got, err := readAll(p)
	if err != nil {
		return fmt.Sprintf("cut at %d of %d: read error %v after %d records", cut, size, err, len(got))
	}
	if len(got) != len(expect) {
		return fmt.Sprintf("cut at %d of %d: %d records back, expected %d", cut, size, len(got), len(expect))
	}
	for i := range got {
		if string(got[i].Key) != string(expect[i].Key) || string(got[i].Value) != string(expect[i].Value) || got[i].Offset != int64(i) {
			return fmt.Sprintf("cut at %d of %d: record %d differs", cut, size, i)
		}
	}
	return ""
}

// ------------------------------------------------------------------ bench

// bench produces over TCP with a minimal client (Produce v3, acks=1) and
// reports messages per second.
func bench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	seconds := fs.Float64("seconds", 5, "duration")
	batch := fs.Int("batch", 100, "records per batch")
	size := fs.Int("size", 100, "value bytes")
	fs.Parse(args)
	dir, _ := os.MkdirTemp("", "tributary-bench-")
	defer os.RemoveAll(dir)
	b, err := broker.New(filepath.Join(dir, "data"), 1)
	if err != nil {
		log.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go b.Serve(ln)
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	recs := make([]record.Record, *batch)
	for i := range recs {
		recs[i] = record.Record{Timestamp: 1700000000000, Value: make([]byte, *size)}
	}
	payload := record.Build(recs, 1700000000000)
	var w wire.Writer
	w.Int16(broker.KeyProduce)
	w.Int16(3)
	w.Int32(1)
	w.String("bench")
	w.NullableString("", false)
	w.Int16(1)
	w.Int32(30000)
	w.ArrayLen(1)
	w.String("bench")
	w.ArrayLen(1)
	w.Int32(0)
	w.Bytes(payload)
	frame := make([]byte, 4, 4+len(w.B))
	binary.BigEndian.PutUint32(frame, uint32(len(w.B)))
	frame = append(frame, w.B...)
	b.TopicFor("bench", true)
	start := time.Now()
	sent := 0
	resp := make([]byte, 4)
	for time.Since(start).Seconds() < *seconds {
		if _, err := c.Write(frame); err != nil {
			log.Fatal(err)
		}
		if _, err := io.ReadFull(c, resp); err != nil {
			log.Fatal(err)
		}
		body := make([]byte, binary.BigEndian.Uint32(resp))
		io.ReadFull(c, body)
		sent += *batch
	}
	el := time.Since(start).Seconds()
	fmt.Printf("produce, 1 connection, acks=1, %d x %d B per batch: %.0f msgs/s, %.1f MB/s\n",
		*batch, *size, float64(sent)/el, float64(sent**size)/el/1e6)
}
