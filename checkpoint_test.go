package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	"modernc.org/sqlite"
)

func TestCheckpointPassive(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"

	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO test (val) VALUES ('data')")
		if err != nil {
			t.Fatal(err)
		}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := sqlite.WALCheckpoint(conn, "main", sqlite.CheckpointPassive)
	if err != nil {
		t.Fatal(err)
	}

	if result.NumFrames < 0 {
		t.Errorf("expected NumFrames >= 0, got %d", result.NumFrames)
	}
	if result.NumCheckpointed < 0 {
		t.Errorf("expected NumCheckpointed >= 0, got %d", result.NumCheckpointed)
	}
}

func TestCheckpointTruncate(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"

	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO test (val) VALUES ('data')")
		if err != nil {
			t.Fatal(err)
		}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := sqlite.WALCheckpoint(conn, "main", sqlite.CheckpointTruncate)
	if err != nil {
		t.Fatal(err)
	}

	if result.NumFrames < 0 {
		t.Errorf("expected NumFrames >= 0, got %d", result.NumFrames)
	}
	// After truncate, all frames should be checkpointed.
	if result.NumCheckpointed != result.NumFrames {
		t.Errorf("expected all frames checkpointed: NumFrames=%d, NumCheckpointed=%d",
			result.NumFrames, result.NumCheckpointed)
	}
}

func TestCheckpointAfterWALHook(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"
	driverName := "sqlite_checkpoint_after_hook_test"

	var testDriver sqlite.Driver
	testDriver.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		if hooker, ok := conn.(sqlite.HookRegisterer); ok {
			hooker.RegisterWALHook(func(dbName string, nFrames int) int {
				return 0 // Accept frames, skip auto-checkpoint.
			})
		}
		return nil
	})

	sql.Register(driverName, &testDriver)
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_, err = db.Exec("INSERT INTO test (val) VALUES ('data')")
		if err != nil {
			t.Fatal(err)
		}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Checkpoint should still work even with WAL hook registered.
	result, err := sqlite.WALCheckpoint(conn, "main", sqlite.CheckpointPassive)
	if err != nil {
		t.Fatal(err)
	}

	if result.NumFrames <= 0 {
		t.Errorf("expected some frames in WAL after writes, got %d", result.NumFrames)
	}
}
