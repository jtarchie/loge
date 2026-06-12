package loge

// Regression benchmark for the ingest hot path (`bind` → push payload decode),
// which a Fly CPU profile showed is ~40% of CPU under max ingest.
//
// A one-off decoder bake-off compared the current goccy/go-json against
// encoding/json, mailru/easyjson (codegen) and a hand-rolled buger/jsonparser
// walk (simdjson-go was excluded — amd64-only, SupportedCPU()=false on arm64).
// goccy's Unmarshal won outright (per 500-line push body, -benchmem):
//
//	goccy Unmarshal  ~0.14 ms  631 MB/s  2487 allocs   <- fastest
//	easyjson (raw)   ~0.21 ms  421 MB/s  ...
//	jsonparser       ~0.32 ms  273 MB/s  ...
//	encoding/json    ~0.48 ms  181 MB/s  4287 allocs
//	goccy Decoder    ~1.47 ms   59 MB/s  ...           <- streaming, 10x slower
//
// So the library was already optimal; the win was switching `bind` from goccy's
// streaming Decoder to ReadAll+Unmarshal. This file keeps the two goccy paths
// (plus stdlib for reference) as a guard so the slow streaming path can't return.
//
// Run: go test -run '^$' -bench BenchmarkDecode -benchmem .

import (
	"bytes"
	encjson "encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	gojson "github.com/goccy/go-json"
)

// fixtureJSON builds a realistic push body: bounded label cardinality, web +
// app lines, the app lines being escape-heavy JSON strings (the worst case for
// string decoding). Mirrors cmd/loge-loadgen.
func fixtureJSON(batch int) []byte {
	r := rand.New(rand.NewSource(42)) //nolint:gosec
	apps := []string{"checkout", "auth", "payments", "catalog", "search", "gateway"}
	envs := []string{"prod", "staging"}
	levels := []string{"info", "warn", "error", "debug"}
	hosts := []string{"host-00", "host-01", "host-02", "host-03", "host-04"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/checkout", "/api/v1/orders", "/login", "/health", "/assets/app.js"}
	msgs := []string{"connection refused", "payment authorized", "rate limit exceeded", "timeout waiting for upstream"}

	grouped := map[string]*Entry{}
	order := []string{}
	hex := func(n int) string {
		const d = "0123456789abcdef"
		b := make([]byte, n)
		for i := range b {
			b[i] = d[r.Intn(16)]
		}
		return string(b)
	}

	for i := 0; i < batch; i++ {
		var labels Stream
		var line string
		if r.Intn(2) == 0 {
			labels = Stream{"service": "nginx", "env": envs[r.Intn(len(envs))], "host": hosts[r.Intn(len(hosts))]}
			line = fmt.Sprintf(`%d.%d.%d.%d - - [10/Jun/2026:15:04:05 -0700] "%s %s HTTP/1.1" %d %d "-" "Mozilla/5.0"`,
				r.Intn(223)+1, r.Intn(256), r.Intn(256), r.Intn(256),
				methods[r.Intn(len(methods))], paths[r.Intn(len(paths))], 200+r.Intn(300), r.Intn(20000))
		} else {
			lvl := levels[r.Intn(len(levels))]
			labels = Stream{"app": apps[r.Intn(len(apps))], "env": envs[r.Intn(len(envs))], "level": lvl, "host": hosts[r.Intn(len(hosts))]}
			line = fmt.Sprintf(`{"level":%q,"msg":%q,"trace_id":%q,"user_id":%q,"latency_ms":%d}`,
				lvl, msgs[r.Intn(len(msgs))], hex(16), hex(8), r.Intn(2000))
		}
		key := labelKeyFor(labels)
		g, ok := grouped[key]
		if !ok {
			g = &Entry{Stream: labels}
			grouped[key] = g
			order = append(order, key)
		}
		ts := strconv.FormatInt(int64(1_700_000_000_000_000_000+i), 10)
		g.Values = append(g.Values, Value{ts, line})
	}

	p := Payload{Streams: make(Streams, 0, len(order))}
	for _, k := range order {
		p.Streams = append(p.Streams, *grouped[k])
	}
	b, err := gojson.Marshal(p)
	if err != nil {
		panic(err)
	}
	return b
}

func labelKeyFor(s Stream) string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(s[k])
		sb.WriteByte(',')
	}
	return sb.String()
}

// decodeGoccy is the production path after the fix: Unmarshal over a full buffer.
func decodeGoccy(b []byte) (*Payload, error) {
	p := &Payload{}
	return p, gojson.Unmarshal(b, p)
}

// decodeGoccyStream is the old bind() path: a streaming decoder over the body.
func decodeGoccyStream(b []byte) (*Payload, error) {
	p := &Payload{}
	return p, gojson.NewDecoder(bytes.NewReader(b)).Decode(p)
}

func decodeStdlib(b []byte) (*Payload, error) {
	p := &Payload{}
	return p, encjson.Unmarshal(b, p)
}

func TestDecodersAgree(t *testing.T) {
	b := fixtureJSON(500)
	want, err := decodeGoccy(b)
	if err != nil {
		t.Fatalf("goccy: %v", err)
	}
	for name, fn := range map[string]func([]byte) (*Payload, error){
		"goccy-stream": decodeGoccyStream,
		"stdlib":       decodeStdlib,
	} {
		got, err := fn(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("%s decoded differently from goccy Unmarshal", name)
		}
	}
}

func benchDecode(b *testing.B, fn func([]byte) (*Payload, error)) {
	data := fixtureJSON(500)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fn(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStdlib(b *testing.B)      { benchDecode(b, decodeStdlib) }
func BenchmarkDecodeGoccy(b *testing.B)       { benchDecode(b, decodeGoccy) }
func BenchmarkDecodeGoccyStream(b *testing.B) { benchDecode(b, decodeGoccyStream) }
