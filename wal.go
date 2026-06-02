package loge

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// The write-ahead log persists each payload to disk (and fsyncs it) before
// /push is acknowledged, giving at-least-once durability: a crash cannot lose
// an acknowledged payload. On startup the log is replayed back through the
// ingest pipeline. Records are framed as:
//
//	[uint32 length][msgpack-encoded Payload][uint32 crc32(payload)]
//
// A torn tail (a partial record from a crash mid-write) is detected by a short
// read or a CRC mismatch and stops replay of that segment.
//
// Each appended record is assigned a monotonically increasing sequence number.
// Segments are sealed (rotated) on size or on demand, and a checkpoint advances
// a "durable" watermark as the pipeline reports which sequences have reached a
// durable, queryable segment; sealed WAL segments entirely below the watermark
// are deleted, so the log stays bounded to the un-flushed tail rather than
// growing for the whole session.
const (
	walHeaderSize            = 4
	walChecksumSize          = 4
	defaultWALMaxSegmentSize = 64 << 20  // 64 MiB
	walMaxRecordSize         = 256 << 20 // sanity bound to reject garbage lengths
)

var errWALClosed = errors.New("wal is closed")

type walAck struct {
	seq uint64
	err error
}

type walRequest struct {
	record []byte
	ack    chan walAck
}

type sealedSegment struct {
	path   string
	maxSeq uint64
}

// WAL is an append-only, group-committing write-ahead log. A single writer
// goroutine batches concurrent appends and fsyncs them together, assigning each
// a sequence number.
type WAL struct {
	dir            string
	maxSegmentSize int64
	requests       chan walRequest
	rotate         chan chan struct{}
	closing        chan struct{}
	closeOnce      sync.Once
	wg             sync.WaitGroup

	mu        sync.Mutex
	sealed    []sealedSegment     // new segments sealed this session, deletable once durable
	recovered []string            // pre-existing segments, removed only by RemoveWAL
	durable   map[uint64]struct{} // flushed seqs above the watermark (gaps)
	watermark uint64              // every seq <= watermark is durable
}

// OpenWAL starts a WAL writer for dir, creating the directory if needed. New
// records are written to fresh segments; any pre-existing segments are recorded
// as "recovered" for ReplaySegments and are only removed by RemoveWAL after a
// clean shutdown.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("could not create wal directory: %w", err)
	}

	recovered, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		return nil, fmt.Errorf("could not list wal segments: %w", err)
	}

	sort.Strings(recovered)

	wal := &WAL{
		dir:            dir,
		maxSegmentSize: defaultWALMaxSegmentSize,
		requests:       make(chan walRequest),
		rotate:         make(chan chan struct{}),
		closing:        make(chan struct{}),
		recovered:      recovered,
		durable:        make(map[uint64]struct{}),
	}

	wal.wg.Add(1)
	go wal.run()

	return wal, nil
}

// Recovered returns the pre-existing segments captured at open time, to be
// replayed before the WAL is used for new writes.
func (w *WAL) Recovered() []string {
	return w.recovered
}

// Append durably writes payload to the log and returns its sequence number once
// it (and any batched concurrent appends) have been fsynced.
func (w *WAL) Append(payload *Payload) (uint64, error) {
	record, err := encodeWALRecord(payload)
	if err != nil {
		return 0, err
	}

	ack := make(chan walAck, 1)

	select {
	case w.requests <- walRequest{record: record, ack: ack}:
	case <-w.closing:
		return 0, errWALClosed
	}

	result := <-ack

	return result.seq, result.err
}

// Rotate seals the active segment (if it holds data) so its records become
// eligible for deletion once they are marked durable.
func (w *WAL) Rotate() {
	done := make(chan struct{})

	select {
	case w.rotate <- done:
		<-done
	case <-w.closing:
	}
}

// MarkDurable records that the given sequence numbers have reached a durable
// segment, advances the contiguous watermark, and deletes any sealed WAL
// segments that lie entirely below it. Sequence 0 (untracked/replayed payloads)
// is ignored.
func (w *WAL) MarkDurable(seqs []uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seq := range seqs {
		if seq != 0 {
			w.durable[seq] = struct{}{}
		}
	}

	for {
		next := w.watermark + 1
		if _, ok := w.durable[next]; !ok {
			break
		}

		delete(w.durable, next)
		w.watermark = next
	}

	kept := w.sealed[:0:0]

	var firstErr error

	for _, segment := range w.sealed {
		if segment.maxSeq <= w.watermark {
			if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
				firstErr = fmt.Errorf("could not remove wal segment %q: %w", segment.path, err)
			}

			continue
		}

		kept = append(kept, segment)
	}

	w.sealed = kept

	return firstErr
}

// Close stops the writer after flushing any queued appends.
func (w *WAL) Close() error {
	w.closeOnce.Do(func() {
		close(w.closing)
	})
	w.wg.Wait()

	return nil
}

func (w *WAL) run() {
	defer w.wg.Done()

	writer := &walWriter{wal: w}
	defer writer.closeActive()

	for {
		select {
		case req := <-w.requests:
			writer.commit(w.collect(req))
		case done := <-w.rotate:
			writer.seal()
			close(done)
		case <-w.closing:
			for {
				select {
				case req := <-w.requests:
					writer.commit(w.collect(req))
				default:
					return
				}
			}
		}
	}
}

// collect gathers the starting request plus any others already queued so they
// share a single fsync.
func (w *WAL) collect(first walRequest) []walRequest {
	batch := []walRequest{first}

	for {
		select {
		case req := <-w.requests:
			batch = append(batch, req)
		default:
			return batch
		}
	}
}

// walWriter owns the active segment file; it is only touched by the run
// goroutine.
type walWriter struct {
	wal     *WAL
	file    *os.File
	path    string
	size    int64
	maxSeq  uint64
	nextSeq uint64
	hasData bool
}

func (ww *walWriter) commit(batch []walRequest) {
	if err := ww.ensureSegment(); err != nil {
		for _, req := range batch {
			req.ack <- walAck{err: err}
		}

		return
	}

	// Write each record, assigning a sequence number only to records that are
	// written successfully (so a failed write never consumes a sequence).
	seqs := make([]uint64, len(batch))
	written := 0

	var writeErr error

	for i := range batch {
		n, err := ww.file.Write(batch[i].record)
		ww.size += int64(n)

		if err != nil {
			writeErr = err

			break
		}

		ww.nextSeq++
		ww.maxSeq = ww.nextSeq
		ww.hasData = true
		seqs[i] = ww.nextSeq
		written++
	}

	// Acknowledge success only after a single fsync covering the batch.
	syncErr := ww.file.Sync()

	for i := range batch {
		switch {
		case i < written && syncErr == nil:
			batch[i].ack <- walAck{seq: seqs[i]}
		case writeErr != nil:
			batch[i].ack <- walAck{err: writeErr}
		default:
			batch[i].ack <- walAck{err: syncErr}
		}
	}
}

func (ww *walWriter) ensureSegment() error {
	if ww.file != nil && ww.size < ww.wal.maxSegmentSize {
		return nil
	}

	ww.seal()

	name := filepath.Join(ww.wal.dir, fmt.Sprintf("wal-%d.log", time.Now().UnixNano()))

	opened, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("could not open wal segment: %w", err)
	}

	ww.file = opened
	ww.path = name
	ww.size = 0
	ww.hasData = false

	return nil
}

// seal closes the active segment and, if it holds data, records it as a sealed
// segment eligible for checkpoint deletion.
func (ww *walWriter) seal() {
	if ww.file == nil {
		return
	}

	_ = ww.file.Sync()
	_ = ww.file.Close()

	if ww.hasData {
		ww.wal.mu.Lock()
		ww.wal.sealed = append(ww.wal.sealed, sealedSegment{path: ww.path, maxSeq: ww.maxSeq})
		ww.wal.mu.Unlock()
	}

	ww.file = nil
	ww.hasData = false
}

func (ww *walWriter) closeActive() {
	if ww.file != nil {
		_ = ww.file.Sync()
		_ = ww.file.Close()
		ww.file = nil
	}
}

func encodeWALRecord(payload *Payload) ([]byte, error) {
	body, err := payload.MarshalMsg(nil)
	if err != nil {
		return nil, fmt.Errorf("could not encode wal payload: %w", err)
	}

	record := make([]byte, walHeaderSize+len(body)+walChecksumSize)
	binary.BigEndian.PutUint32(record[:walHeaderSize], uint32(len(body)))
	copy(record[walHeaderSize:], body)
	binary.BigEndian.PutUint32(record[walHeaderSize+len(body):], crc32.ChecksumIEEE(body))

	return record, nil
}

// ReplaySegments reads the given segments in order and invokes fn for each
// intact record. A torn tail (short read or CRC mismatch) ends a segment's
// replay without erroring, since it represents an interrupted write. It returns
// the number of records replayed.
func ReplaySegments(segments []string, fn func(*Payload)) (int, error) {
	replayed := 0

	for _, segment := range segments {
		count, err := replaySegment(segment, fn)
		replayed += count

		if err != nil {
			return replayed, err
		}
	}

	return replayed, nil
}

// ReplayWAL replays every segment found in dir.
func ReplayWAL(dir string, fn func(*Payload)) (int, error) {
	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		return 0, fmt.Errorf("could not list wal segments: %w", err)
	}

	sort.Strings(segments)

	return ReplaySegments(segments, fn)
}

func replaySegment(path string, fn func(*Payload)) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("could not open wal segment %q: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	reader := bufio.NewReader(file)
	replayed := 0

	for {
		header := make([]byte, walHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			// Clean EOF or a torn tail: stop replaying this segment.
			return replayed, nil
		}

		length := binary.BigEndian.Uint32(header)
		if length == 0 || length > walMaxRecordSize {
			return replayed, nil
		}

		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			return replayed, nil
		}

		checksum := make([]byte, walChecksumSize)
		if _, err := io.ReadFull(reader, checksum); err != nil {
			return replayed, nil
		}

		if binary.BigEndian.Uint32(checksum) != crc32.ChecksumIEEE(body) {
			return replayed, nil
		}

		payload := &Payload{}
		if _, err := payload.UnmarshalMsg(body); err != nil {
			// A decode failure on an otherwise CRC-valid record is unexpected;
			// surface it rather than silently dropping data.
			return replayed, fmt.Errorf("could not decode wal record in %q: %w", path, err)
		}

		fn(payload)
		replayed++
	}
}

// RemoveWAL deletes all log segments in dir. It is safe to call only after the
// ingest pipeline has durably flushed every replayed and appended payload
// (i.e. after a clean shutdown).
func RemoveWAL(dir string) error {
	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("could not list wal segments: %w", err)
	}

	for _, segment := range segments {
		if err := os.Remove(segment); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("could not remove wal segment %q: %w", segment, err)
		}
	}

	return nil
}
