package sqlite

import (
	"database/sql"
	"fmt"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

var _ CheckpointController = (*conn)(nil)

// CheckpointMode controls the behavior of WALCheckpoint.
type CheckpointMode int

const (
	// CheckpointPassive checkpoints as many frames as possible without waiting
	// for readers or writers to finish. The busy-handler is not invoked.
	CheckpointPassive CheckpointMode = 0 // SQLITE_CHECKPOINT_PASSIVE

	// CheckpointFull blocks until there are no database writers, then
	// checkpoints all frames. It blocks on writers but not readers.
	CheckpointFull CheckpointMode = 1 // SQLITE_CHECKPOINT_FULL

	// CheckpointRestart is like CheckpointFull but also blocks until all
	// readers are reading from the database file. This ensures the next writer
	// will restart the WAL file from the beginning.
	CheckpointRestart CheckpointMode = 2 // SQLITE_CHECKPOINT_RESTART

	// CheckpointTruncate is like CheckpointRestart but also truncates the
	// WAL file to zero bytes.
	CheckpointTruncate CheckpointMode = 3 // SQLITE_CHECKPOINT_TRUNCATE
)

// CheckpointResult contains the result of a WAL checkpoint operation.
type CheckpointResult struct {
	NumFrames       int // total frames in WAL
	NumCheckpointed int // frames successfully checkpointed to DB
}

// WALCheckpoint runs a WAL checkpoint on the named database (typically "main").
// Pass an empty string for dbName to checkpoint the main database.
func (c *conn) WALCheckpoint(dbName string, mode CheckpointMode) (CheckpointResult, error) {
	var zDb uintptr
	if dbName != "" {
		var err error
		zDb, err = libc.CString(dbName)
		if err != nil {
			return CheckpointResult{}, err
		}
		defer c.free(zDb)
	}

	pnLog := c.tls.Alloc(4)
	defer c.tls.Free(4)
	pnCkpt := c.tls.Alloc(4)
	defer c.tls.Free(4)

	rc := sqlite3.Xsqlite3_wal_checkpoint_v2(c.tls, c.db, zDb, int32(mode), pnLog, pnCkpt)
	if rc != sqlite3.SQLITE_OK {
		return CheckpointResult{}, c.errstr(rc)
	}

	nLog := *(*int32)(unsafe.Pointer(pnLog))
	nCkpt := *(*int32)(unsafe.Pointer(pnCkpt))

	return CheckpointResult{
		NumFrames:       int(nLog),
		NumCheckpointed: int(nCkpt),
	}, nil
}

// WALCheckpoint runs a WAL checkpoint on a *sql.Conn obtained from *sql.DB.Conn().
func WALCheckpoint(c *sql.Conn, dbName string, mode CheckpointMode) (CheckpointResult, error) {
	var result CheckpointResult
	err := c.Raw(func(driverConn any) error {
		switch dc := driverConn.(type) {
		case *conn:
			var err error
			result, err = dc.WALCheckpoint(dbName, mode)
			return err
		default:
			return fmt.Errorf("unexpected driverConn type: %T", driverConn)
		}
	})
	return result, err
}
