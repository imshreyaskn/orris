package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type OpType string

const (
	OpSet    OpType = "SET"
	OpDelete OpType = "DELETE"
)

type LogEntry struct {
	Index     int
	Term      int
	Operation OpType
	Key       string
	Value     string
}

type RecordType string

const (
	RecordState    RecordType = "STATE"
	RecordEntry    RecordType = "ENTRY"
	RecordTruncate RecordType = "TRUNCATE"
	RecordCommit   RecordType = "COMMIT"
)

type WALRecord struct {
	Type        RecordType
	Term        int
	VotedFor    string
	CommitIndex int
	Entry       *LogEntry
}

var (
	ErrCorruptRecord = errors.New("wal: corrupt record or invalid checksum")
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// OpenWAL opens or creates the write-ahead log file and positions at EOF for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end so subsequent appends don't overwrite if reopened
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		_ = w.file.Sync()
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// Append writes a length-prefixed, CRC32-checksummed gob-encoded record and fsyncs.
func (w *WAL) Append(rec WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return os.ErrClosed
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rec); err != nil {
		return err
	}

	data := buf.Bytes()
	checksum := crc32.ChecksumIEEE(data)

	// Format: [4 bytes payload length][4 bytes CRC32][payload bytes]
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

// ReadAll reads all valid records from the start of the WAL file.
// It stops on EOF or any corruption/incomplete write from an ungraceful crash.
func (w *WAL) ReadAll() ([]WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, os.ErrClosed
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []WALRecord
	header := make([]byte, 8)

	for {
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return records, err
		}

		length := binary.BigEndian.Uint32(header[0:4])
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		_, err = io.ReadFull(w.file, data)
		if err != nil {
			// Incomplete record due to crash
			break
		}

		if crc32.ChecksumIEEE(data) != expectedChecksum {
			// Corrupt record, stop replay here safely
			break
		}

		var rec WALRecord
		dec := gob.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&rec); err != nil {
			break
		}
		records = append(records, rec)
	}

	// Reposition write head at end of file
	_, _ = w.file.Seek(0, io.SeekEnd)
	return records, nil
}
