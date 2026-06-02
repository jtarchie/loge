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
const (
	walHeaderSize            = 4
	walChecksumSize          = 4
	defaultWALMaxSegmentSize = 64 << 20 // 64 MiB
	walMaxRecordSize         = 256 << 20 // sanity bound to reject garbage lengths
)

var errWALClosed = errors.New("wal is closed")

type walRequest struct {
	record []byte
	ack    chan error
}

// WAL is an append-only, group-committing write-ahead log. A single writer
// goroutine batches concurrent appends and fsyncs them together.
type WAL struct {
	dir            string
	maxSegmentSize int64
	requests       chan walRequest
	closing        chan struct{}
	closeOnce      sync.Once
	wg             sync.WaitGroup
}

// OpenWAL starts a WAL writer for dir, creating the directory if needed. New
// records are written to a fresh segment; any pre-existing segments are left in
// place for ReplayWAL and are only removed by RemoveWAL after a clean shutdown.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("could not create wal directory: %w", err)
	}

	wal := &WAL{
		dir:            dir,
		maxSegmentSize: defaultWALMaxSegmentSize,
		requests:       make(chan walRequest),
		closing:        make(chan struct{}),
	}

	wal.wg.Add(1)
	go wal.run()

	return wal, nil
}

// Append durably writes payload to the log and returns only once it (and any
// batched concurrent appends) have been fsynced.
func (w *WAL) Append(payload *Payload) error {
	record, err := encodeWALRecord(payload)
	if err != nil {
		return err
	}

	ack := make(chan error, 1)

	select {
	case w.requests <- walRequest{record: record, ack: ack}:
	case <-w.closing:
		return errWALClosed
	}

	return <-ack
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

	var (
		file *os.File
		size int64
	)

	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()

	for {
		select {
		case req := <-w.requests:
			w.commit(&file, &size, w.collect(req))
		case <-w.closing:
			// Drain anything already queued, then exit.
			for {
				select {
				case req := <-w.requests:
					w.commit(&file, &size, w.collect(req))
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

// commit writes a batch to the active segment, fsyncs once, then acks all.
func (w *WAL) commit(file **os.File, size *int64, batch []walRequest) {
	err := w.ensureSegment(file, size)
	if err == nil {
		for _, req := range batch {
			n, werr := (*file).Write(req.record)
			*size += int64(n)

			if werr != nil {
				err = werr

				break
			}
		}
	}

	if err == nil && *file != nil {
		err = (*file).Sync()
	}

	for _, req := range batch {
		req.ack <- err
	}
}

// ensureSegment opens the first segment, or rotates to a new one once the
// active segment exceeds the size limit.
func (w *WAL) ensureSegment(file **os.File, size *int64) error {
	if *file != nil && *size < w.maxSegmentSize {
		return nil
	}

	if *file != nil {
		_ = (*file).Close()
		*file = nil
	}

	name := filepath.Join(w.dir, fmt.Sprintf("wal-%d.log", time.Now().UnixNano()))

	opened, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("could not open wal segment: %w", err)
	}

	*file = opened
	*size = 0

	return nil
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

// ReplayWAL reads every segment in dir in order and invokes fn for each intact
// record. A torn tail (short read or CRC mismatch) ends a segment's replay
// without erroring, since it represents an interrupted write. It returns the
// number of records replayed.
func ReplayWAL(dir string, fn func(*Payload)) (int, error) {
	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		return 0, fmt.Errorf("could not list wal segments: %w", err)
	}

	sort.Strings(segments)

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
