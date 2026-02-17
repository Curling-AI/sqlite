package sqlite_test

import (
	"database/sql"
	"sync"
	"testing"

	"modernc.org/sqlite"
)

func TestWALHook(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"
	driverName := "sqlite_wal_hook_test"

	var mu sync.Mutex
	var hookCalls []struct {
		dbName  string
		nFrames int
	}

	var testDriver sqlite.Driver
	testDriver.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		if hooker, ok := conn.(sqlite.HookRegisterer); ok {
			hooker.RegisterWALHook(func(dbName string, nFrames int) int {
				mu.Lock()
				hookCalls = append(hookCalls, struct {
					dbName  string
					nFrames int
				}{dbName, nFrames})
				mu.Unlock()
				return 0
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

	// Force a single connection so hook fires on it.
	db.SetMaxOpenConns(1)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO test (val) VALUES ('hello')")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO test (val) VALUES ('world')")
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(hookCalls) < 3 {
		t.Fatalf("expected at least 3 WAL hook calls, got %d", len(hookCalls))
	}

	for i, call := range hookCalls {
		if call.dbName != "main" {
			t.Errorf("hook call %d: expected dbName 'main', got %q", i, call.dbName)
		}
		if call.nFrames <= 0 {
			t.Errorf("hook call %d: expected nFrames > 0, got %d", i, call.nFrames)
		}
	}
}

func TestWALHookRemovalRestoresAutocheckpoint(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"
	driverName := "sqlite_wal_hook_remove_test"

	var hookCalled bool

	var testDriver sqlite.Driver
	testDriver.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		if hooker, ok := conn.(sqlite.HookRegisterer); ok {
			// Register, then immediately remove.
			hooker.RegisterWALHook(func(dbName string, nFrames int) int {
				hookCalled = true
				return 0
			})
			hooker.RegisterWALHook(nil) // remove
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

	_, err = db.Exec("INSERT INTO test (val) VALUES ('test')")
	if err != nil {
		t.Fatal(err)
	}

	if hookCalled {
		t.Error("WAL hook should not fire after removal")
	}
}

func TestWALHookFrameAccumulation(t *testing.T) {
	dir := t.TempDir()
	connStr := dir + "/test.db?_pragma=journal_mode(wal)"
	driverName := "sqlite_wal_hook_accum_test"

	var mu sync.Mutex
	var frameCounts []int

	var testDriver sqlite.Driver
	testDriver.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		if hooker, ok := conn.(sqlite.HookRegisterer); ok {
			hooker.RegisterWALHook(func(dbName string, nFrames int) int {
				mu.Lock()
				frameCounts = append(frameCounts, nFrames)
				mu.Unlock()
				return 0
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

	// Multiple inserts should accumulate frames since WAL hook disables auto-checkpoint.
	for i := 0; i < 5; i++ {
		_, err = db.Exec("INSERT INTO test (val) VALUES ('data')")
		if err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(frameCounts) < 6 {
		t.Fatalf("expected at least 6 frame count reports, got %d", len(frameCounts))
	}

	// Frame counts should be non-decreasing (they accumulate without checkpoint).
	for i := 1; i < len(frameCounts); i++ {
		if frameCounts[i] < frameCounts[i-1] {
			t.Errorf("frame counts should be non-decreasing: index %d has %d, index %d has %d",
				i-1, frameCounts[i-1], i, frameCounts[i])
		}
	}
}
