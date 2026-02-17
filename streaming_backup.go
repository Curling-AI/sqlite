package sqlite

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	sqlite3 "modernc.org/sqlite/lib"
)

var _ StreamingBackuper = (*conn)(nil)

const (
	snapshotMagic   = 0x534E4150
	snapshotVersion = 1
)

// SnapshotHeader is the header written at the beginning of a streaming backup.
type SnapshotHeader struct {
	Magic     uint32 // 0x534E4150 ("SNAP")
	Version   uint32 // 1
	PageSize  uint32
	PageCount uint32
	DBSize    int64
}

// StreamingBackup writes a consistent snapshot of the database to w.
// It uses sqlite3_backup to copy the database to an in-memory database,
// serializes it, and streams the result to the writer.
//
// The optional progressFn is called during the backup with the remaining
// and total page counts.
func (c *conn) StreamingBackup(w io.Writer, progressFn func(remaining, total int)) error {
	// Create in-memory destination.
	dstConn, err := newConn(":memory:")
	if err != nil {
		return fmt.Errorf("streaming backup: creating memory db: %w", err)
	}
	defer dstConn.Close()

	srcSchema := sqlite3.Xsqlite3_db_name(c.tls, c.db, 0)
	if srcSchema == 0 {
		return fmt.Errorf("streaming backup: failed to get source db name")
	}

	dstSchema := sqlite3.Xsqlite3_db_name(dstConn.tls, dstConn.db, 0)
	if dstSchema == 0 {
		return fmt.Errorf("streaming backup: failed to get destination db name")
	}

	pBackup := sqlite3.Xsqlite3_backup_init(c.tls, dstConn.db, dstSchema, c.db, srcSchema)
	if pBackup == 0 {
		rc := sqlite3.Xsqlite3_errcode(c.tls, dstConn.db)
		return c.errstr(rc)
	}

	// Copy all pages in steps.
	for {
		rc := sqlite3.Xsqlite3_backup_step(c.tls, pBackup, 100)
		if progressFn != nil {
			remaining := int(sqlite3.Xsqlite3_backup_remaining(c.tls, pBackup))
			total := int(sqlite3.Xsqlite3_backup_pagecount(c.tls, pBackup))
			progressFn(remaining, total)
		}
		if rc == sqlite3.SQLITE_DONE {
			break
		}
		if rc != sqlite3.SQLITE_OK {
			sqlite3.Xsqlite3_backup_finish(c.tls, pBackup)
			return c.errstr(rc)
		}
	}

	rc := sqlite3.Xsqlite3_backup_finish(c.tls, pBackup)
	if rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}

	// Serialize the in-memory database.
	data, err := dstConn.Serialize()
	if err != nil {
		return fmt.Errorf("streaming backup: serialize: %w", err)
	}

	// Determine page size from database header.
	pageSize := uint32(0)
	if len(data) >= 100 {
		// SQLite database header: page size at offset 16, 2 bytes big-endian.
		ps := binary.BigEndian.Uint16(data[16:18])
		if ps == 1 {
			pageSize = 65536
		} else {
			pageSize = uint32(ps)
		}
	}

	pageCount := uint32(0)
	if pageSize > 0 {
		pageCount = uint32(len(data)) / pageSize
	}

	// Write snapshot header.
	hdr := SnapshotHeader{
		Magic:     snapshotMagic,
		Version:   snapshotVersion,
		PageSize:  pageSize,
		PageCount: pageCount,
		DBSize:    int64(len(data)),
	}

	if err := binary.Write(w, binary.LittleEndian, &hdr); err != nil {
		return fmt.Errorf("streaming backup: writing header: %w", err)
	}

	// Write database contents.
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("streaming backup: writing data: %w", err)
	}

	return nil
}

// StreamingRestore restores a database from a snapshot written by StreamingBackup.
// It writes the snapshot to a temporary file, opens it as a SQLite database, and
// uses sqlite3_backup to copy it to the current connection.
func (c *conn) StreamingRestore(r io.Reader) error {
	var hdr SnapshotHeader
	if err := binary.Read(r, binary.LittleEndian, &hdr); err != nil {
		return fmt.Errorf("streaming restore: reading header: %w", err)
	}

	if hdr.Magic != snapshotMagic {
		return fmt.Errorf("streaming restore: invalid magic: 0x%08X", hdr.Magic)
	}
	if hdr.Version != snapshotVersion {
		return fmt.Errorf("streaming restore: unsupported version: %d", hdr.Version)
	}
	if hdr.DBSize <= 0 {
		return fmt.Errorf("streaming restore: invalid database size: %d", hdr.DBSize)
	}

	data := make([]byte, hdr.DBSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return fmt.Errorf("streaming restore: reading data: %w", err)
	}

	// Write to a temporary file so we can open it as a proper SQLite database.
	tmpDir, err := os.MkdirTemp("", "sqlite-restore-*")
	if err != nil {
		return fmt.Errorf("streaming restore: creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tmpPath := filepath.Join(tmpDir, "restore.db")
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("streaming restore: writing temp file: %w", err)
	}

	// Open the temp file as a SQLite database.
	srcConn, err := newConn(tmpPath)
	if err != nil {
		return fmt.Errorf("streaming restore: opening temp db: %w", err)
	}
	defer srcConn.Close()

	// Use backup API to copy from temp DB to this connection.
	dstSchema := sqlite3.Xsqlite3_db_name(c.tls, c.db, 0)
	if dstSchema == 0 {
		return fmt.Errorf("streaming restore: failed to get destination db name")
	}

	srcSchema := sqlite3.Xsqlite3_db_name(srcConn.tls, srcConn.db, 0)
	if srcSchema == 0 {
		return fmt.Errorf("streaming restore: failed to get source db name")
	}

	pBackup := sqlite3.Xsqlite3_backup_init(c.tls, c.db, dstSchema, srcConn.db, srcSchema)
	if pBackup == 0 {
		rc := sqlite3.Xsqlite3_errcode(c.tls, c.db)
		return c.errstr(rc)
	}

	rc := sqlite3.Xsqlite3_backup_step(c.tls, pBackup, -1) // copy all at once
	finishRc := sqlite3.Xsqlite3_backup_finish(c.tls, pBackup)

	if rc != sqlite3.SQLITE_DONE {
		return c.errstr(rc)
	}
	if finishRc != sqlite3.SQLITE_OK {
		return c.errstr(finishRc)
	}

	return nil
}
