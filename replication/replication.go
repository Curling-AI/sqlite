// Package replication provides WAL (Write-Ahead Log) parsing and writing
// utilities for SQLite WAL-based replication.
//
// All integers in WAL files are stored big-endian regardless of the magic
// number. The magic number only affects the byte order used when reading
// words for checksum computation:
//   - 0x377F0682: checksum reads words as little-endian
//   - 0x377F0683: checksum reads words as big-endian
package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	// WALHeaderSize is the size of a WAL file header in bytes.
	WALHeaderSize = 32

	// WALFrameHeaderSize is the size of a WAL frame header in bytes.
	WALFrameHeaderSize = 24
)

// WAL magic numbers.
const (
	WALMagicLE = 0x377F0682 // little-endian checksum
	WALMagicBE = 0x377F0683 // big-endian checksum
)

var (
	ErrInvalidWALHeader   = errors.New("replication: invalid WAL header")
	ErrInvalidFrameHeader = errors.New("replication: invalid frame header")
	ErrBadMagic           = errors.New("replication: bad WAL magic number")
)

// WALHeader represents a parsed SQLite WAL file header.
type WALHeader struct {
	Magic         uint32
	FormatVersion uint32
	PageSize      uint32
	CheckpointSeq uint32
	Salt1         uint32
	Salt2         uint32
	Checksum1     uint32
	Checksum2     uint32
}

// WALFrameHeader represents a parsed SQLite WAL frame header.
type WALFrameHeader struct {
	PageNumber uint32
	DBSize     uint32
	Salt1      uint32
	Salt2      uint32
	Checksum1  uint32
	Checksum2  uint32
}

// IsCommit returns true if this frame is a commit frame.
// A commit frame has a non-zero DBSize field indicating the database size
// in pages after the transaction commits.
func (fh WALFrameHeader) IsCommit() bool {
	return fh.DBSize > 0
}

// WALFrame represents a complete WAL frame (header + page data).
type WALFrame struct {
	Header WALFrameHeader
	Data   []byte
}

// ParseWALHeader parses a 32-byte WAL file header.
func ParseWALHeader(b []byte) (WALHeader, error) {
	if len(b) < WALHeaderSize {
		return WALHeader{}, ErrInvalidWALHeader
	}

	h := WALHeader{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		FormatVersion: binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Checksum1:     binary.BigEndian.Uint32(b[24:28]),
		Checksum2:     binary.BigEndian.Uint32(b[28:32]),
	}

	if h.Magic != WALMagicLE && h.Magic != WALMagicBE {
		return WALHeader{}, fmt.Errorf("%w: 0x%x", ErrBadMagic, h.Magic)
	}

	return h, nil
}

// ParseFrameHeader parses a 24-byte WAL frame header.
func ParseFrameHeader(b []byte) (WALFrameHeader, error) {
	if len(b) < WALFrameHeaderSize {
		return WALFrameHeader{}, ErrInvalidFrameHeader
	}

	return WALFrameHeader{
		PageNumber: binary.BigEndian.Uint32(b[0:4]),
		DBSize:     binary.BigEndian.Uint32(b[4:8]),
		Salt1:      binary.BigEndian.Uint32(b[8:12]),
		Salt2:      binary.BigEndian.Uint32(b[12:16]),
		Checksum1:  binary.BigEndian.Uint32(b[16:20]),
		Checksum2:  binary.BigEndian.Uint32(b[20:24]),
	}, nil
}

// MarshalWALHeader serializes a WALHeader to 32 bytes (big-endian).
func MarshalWALHeader(h WALHeader) []byte {
	b := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], h.Magic)
	binary.BigEndian.PutUint32(b[4:8], h.FormatVersion)
	binary.BigEndian.PutUint32(b[8:12], h.PageSize)
	binary.BigEndian.PutUint32(b[12:16], h.CheckpointSeq)
	binary.BigEndian.PutUint32(b[16:20], h.Salt1)
	binary.BigEndian.PutUint32(b[20:24], h.Salt2)
	binary.BigEndian.PutUint32(b[24:28], h.Checksum1)
	binary.BigEndian.PutUint32(b[28:32], h.Checksum2)
	return b
}

// MarshalFrameHeader serializes a WALFrameHeader to 24 bytes (big-endian).
func MarshalFrameHeader(fh WALFrameHeader) []byte {
	b := make([]byte, WALFrameHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], fh.PageNumber)
	binary.BigEndian.PutUint32(b[4:8], fh.DBSize)
	binary.BigEndian.PutUint32(b[8:12], fh.Salt1)
	binary.BigEndian.PutUint32(b[12:16], fh.Salt2)
	binary.BigEndian.PutUint32(b[16:20], fh.Checksum1)
	binary.BigEndian.PutUint32(b[20:24], fh.Checksum2)
	return b
}

// walChecksum computes the SQLite WAL checksum over data, updating the
// running checksum values s0 and s1.
//
// The nativeCksum flag determines the byte order used to read 32-bit words
// from the data for checksum computation:
//   - true  (magic 0x377F0682): read words as little-endian
//   - false (magic 0x377F0683): read words as big-endian
func walChecksum(nativeCksum bool, data []byte, s0, s1 uint32) (uint32, uint32) {
	var order binary.ByteOrder
	if nativeCksum {
		order = binary.LittleEndian
	} else {
		order = binary.BigEndian
	}
	for i := 0; i+7 < len(data); i += 8 {
		s0 += order.Uint32(data[i:i+4]) + s1
		s1 += order.Uint32(data[i+4:i+8]) + s0
	}
	return s0, s1
}

// WALWriter writes WAL frames to a file with correct checksum computation.
type WALWriter struct {
	file        *os.File
	header      WALHeader
	offset      int64
	s0, s1      uint32 // running checksum state
	nativeCksum bool
}

// NewWALWriter creates a new WALWriter that writes a fresh WAL file.
// It writes the WAL header (with computed checksum) and positions for
// frame writes.
func NewWALWriter(file *os.File, header WALHeader) (*WALWriter, error) {
	nativeCksum := header.Magic == WALMagicLE

	// Marshal header bytes 0..23 for checksum (excludes the checksum fields).
	hdrBytes := make([]byte, 24)
	binary.BigEndian.PutUint32(hdrBytes[0:4], header.Magic)
	binary.BigEndian.PutUint32(hdrBytes[4:8], header.FormatVersion)
	binary.BigEndian.PutUint32(hdrBytes[8:12], header.PageSize)
	binary.BigEndian.PutUint32(hdrBytes[12:16], header.CheckpointSeq)
	binary.BigEndian.PutUint32(hdrBytes[16:20], header.Salt1)
	binary.BigEndian.PutUint32(hdrBytes[20:24], header.Salt2)

	s0, s1 := walChecksum(nativeCksum, hdrBytes, 0, 0)
	header.Checksum1 = s0
	header.Checksum2 = s1

	fullHeader := MarshalWALHeader(header)
	if _, err := file.WriteAt(fullHeader, 0); err != nil {
		return nil, fmt.Errorf("replication: failed to write WAL header: %w", err)
	}

	return &WALWriter{
		file:        file,
		header:      header,
		offset:      WALHeaderSize,
		s0:          s0,
		s1:          s1,
		nativeCksum: nativeCksum,
	}, nil
}

// NewWALWriterAppend creates a WALWriter that appends to an existing WAL file.
// offset is the byte position to write the next frame, and s0/s1 are the
// running checksum values from scanning existing frames.
func NewWALWriterAppend(file *os.File, header WALHeader, offset int64, s0, s1 uint32) *WALWriter {
	return &WALWriter{
		file:        file,
		header:      header,
		offset:      offset,
		s0:          s0,
		s1:          s1,
		nativeCksum: header.Magic == WALMagicLE,
	}
}

// WriteFrame writes a single WAL frame (header + page data) and updates
// the running checksum. Returns the new checksum values (s0, s1).
func (w *WALWriter) WriteFrame(frame *WALFrame) (uint32, uint32, error) {
	// Build the 8-byte frame header prefix (pageNumber + dbSize) in big-endian
	// for checksum computation.
	frameHdrFirst8 := make([]byte, 8)
	binary.BigEndian.PutUint32(frameHdrFirst8[0:4], frame.Header.PageNumber)
	binary.BigEndian.PutUint32(frameHdrFirst8[4:8], frame.Header.DBSize)

	// Compute checksum over frame header prefix + page data.
	s0, s1 := walChecksum(w.nativeCksum, frameHdrFirst8, w.s0, w.s1)
	s0, s1 = walChecksum(w.nativeCksum, frame.Data, s0, s1)

	// Build the full 24-byte frame header with salt and checksum.
	fh := WALFrameHeader{
		PageNumber: frame.Header.PageNumber,
		DBSize:     frame.Header.DBSize,
		Salt1:      w.header.Salt1,
		Salt2:      w.header.Salt2,
		Checksum1:  s0,
		Checksum2:  s1,
	}

	hdrBytes := MarshalFrameHeader(fh)

	// Write frame header + page data.
	if _, err := w.file.WriteAt(hdrBytes, w.offset); err != nil {
		return w.s0, w.s1, fmt.Errorf("replication: failed to write frame header: %w", err)
	}
	if _, err := w.file.WriteAt(frame.Data, w.offset+WALFrameHeaderSize); err != nil {
		return w.s0, w.s1, fmt.Errorf("replication: failed to write frame data: %w", err)
	}

	// Update state.
	w.s0 = s0
	w.s1 = s1
	w.offset += WALFrameHeaderSize + int64(len(frame.Data))

	return s0, s1, nil
}

// Sync flushes the WAL file to disk.
func (w *WALWriter) Sync() error {
	return w.file.Sync()
}
