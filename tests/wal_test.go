package tests

import (
	"os"
	"path/filepath"
	"testing"

	"orris/internal/storage"
)

func TestWALPersistence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := storage.LogEntry{Index: 0, Term: 1, Operation: storage.OpSet, Key: "user", Value: "alice"}
	entry2 := storage.LogEntry{Index: 1, Term: 1, Operation: storage.OpSet, Key: "role", Value: "admin"}

	if err := wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &entry1}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &entry2}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(storage.WALRecord{Type: storage.RecordState, Term: 1, VotedFor: "node1"}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(storage.WALRecord{Type: storage.RecordCommit, CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Re-open and verify all 4 records are preserved
	wal2, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	records, err := wal2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 4 {
		t.Fatalf("Expected 4 records, got %d", len(records))
	}

	if records[0].Entry == nil || records[0].Entry.Key != "user" || records[0].Entry.Value != "alice" {
		t.Fatalf("Unexpected first record: %+v", records[0])
	}

	if records[2].VotedFor != "node1" || records[2].Term != 1 {
		t.Fatalf("Unexpected state record: %+v", records[2])
	}

	if records[3].CommitIndex != 1 {
		t.Fatalf("Unexpected commit record: %+v", records[3])
	}
}

func TestWALCorruptionResilience(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "corrupt_wal.log")

	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := storage.LogEntry{Index: 0, Term: 1, Operation: storage.OpSet, Key: "k1", Value: "v1"}
	entry2 := storage.LogEntry{Index: 1, Term: 1, Operation: storage.OpSet, Key: "k2", Value: "v2"}

	_ = wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &entry1})
	_ = wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &entry2})
	_ = wal.Close()

	// Corrupt bytes at the end of the file
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) > 10 {
		// Flip a bit in the second record payload
		data[len(data)-5] ^= 0xFF
		if err := os.WriteFile(walPath, data, 0644); err != nil {
			t.Fatal(err)
		}
	}

	// ReadAll should safely return the valid first record and stop at the corrupt record without panic
	walCorrupt, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer walCorrupt.Close()

	records, err := walCorrupt.ReadAll()
	if err != nil {
		t.Fatalf("Expected graceful recovery, got err: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 valid record before corruption, got %d", len(records))
	}
	if records[0].Entry.Key != "k1" {
		t.Fatalf("Expected key k1, got %s", records[0].Entry.Key)
	}
}
