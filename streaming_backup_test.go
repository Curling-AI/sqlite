package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"

	"modernc.org/sqlite"
)

func TestStreamingBackupRestore(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	// Create and populate a table.
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO test (val) VALUES (?)", fmt.Sprintf("data-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Stream backup via Raw().
	var buf bytes.Buffer
	var progressCalled bool

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Raw(func(driverConn any) error {
		if backuper, ok := driverConn.(sqlite.StreamingBackuper); ok {
			return backuper.StreamingBackup(&buf, func(remaining, total int) {
				progressCalled = true
				if total <= 0 {
					t.Errorf("total should be > 0, got %d", total)
				}
			})
		}
		return fmt.Errorf("connection does not implement StreamingBackuper")
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !progressCalled {
		t.Error("progress callback was not called")
	}

	if buf.Len() == 0 {
		t.Fatal("backup produced no data")
	}

	// Restore to a new database.
	dir := t.TempDir()
	db2, err := sql.Open("sqlite", dir+"/restored.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	db2.SetMaxOpenConns(1)

	conn2, err := db2.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = conn2.Raw(func(driverConn any) error {
		if backuper, ok := driverConn.(sqlite.StreamingBackuper); ok {
			return backuper.StreamingRestore(bytes.NewReader(buf.Bytes()))
		}
		return fmt.Errorf("connection does not implement StreamingBackuper")
	})
	conn2.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Verify data.
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}

	// Verify a specific value.
	var val string
	err = db2.QueryRow("SELECT val FROM test WHERE id = 50").Scan(&val)
	if err != nil {
		t.Fatal(err)
	}
	if val != "data-49" {
		t.Errorf("expected 'data-49', got %q", val)
	}
}

func TestStreamingBackupEmpty(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err = conn.Raw(func(driverConn any) error {
		if backuper, ok := driverConn.(sqlite.StreamingBackuper); ok {
			return backuper.StreamingBackup(&buf, nil)
		}
		return fmt.Errorf("connection does not implement StreamingBackuper")
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	if buf.Len() == 0 {
		t.Fatal("even empty database should produce some data")
	}
}
