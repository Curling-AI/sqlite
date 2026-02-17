package sqlite

import (
	"sync"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

var xWALHandlers = struct {
	mu sync.RWMutex
	m  map[uintptr]WALHookFn
}{
	m: make(map[uintptr]WALHookFn),
}

// WALHookFn is the callback type for WAL commit hooks. It fires after each
// WAL commit with the database name and the number of frames in the WAL.
// The return value is passed back to SQLite (conventionally SQLITE_OK = 0).
type WALHookFn func(dbName string, nFrames int) int

// RegisterWALHook registers a callback to be invoked after each WAL commit.
//
// Registering a WAL hook replaces SQLite's default auto-checkpoint behavior.
// When the hook is removed (nil callback), auto-checkpoint is restored with
// the default threshold of 1000 frames.
//
// The callback must not call back into SQLite on the same connection
// (it fires inside the commit path).
func (c *conn) RegisterWALHook(callback WALHookFn) {
	if callback == nil {
		xWALHandlers.mu.Lock()
		delete(xWALHandlers.m, c.db)
		xWALHandlers.mu.Unlock()
		sqlite3.Xsqlite3_wal_hook(c.tls, c.db, uintptr(unsafe.Pointer(nil)), uintptr(unsafe.Pointer(nil)))
		// Restore default auto-checkpoint behavior.
		sqlite3.Xsqlite3_wal_autocheckpoint(c.tls, c.db, 1000)
		return
	}
	xWALHandlers.mu.Lock()
	xWALHandlers.m[c.db] = callback
	xWALHandlers.mu.Unlock()

	sqlite3.Xsqlite3_wal_hook(c.tls, c.db, cFuncPointer(walHookTrampoline), c.db)
}

// walHookTrampoline bridges the C callback to the registered Go WALHookFn.
// Signature matches sqlite3_wal_hook callback:
//
//	int (*)(void *pArg, sqlite3 *db, const char *zDb, int nFrame)
func walHookTrampoline(tls *libc.TLS, handle uintptr, db uintptr, zDb uintptr, nFrame int32) int32 {
	xWALHandlers.mu.RLock()
	handler := xWALHandlers.m[handle]
	xWALHandlers.mu.RUnlock()

	if handler == nil {
		return 0
	}

	dbName := libc.GoString(zDb)
	return int32(handler(dbName, int(nFrame)))
}
