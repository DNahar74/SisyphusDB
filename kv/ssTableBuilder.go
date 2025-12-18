package kv

import (
	"KV-Store/bloom"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"
)

const blockSize = 4 * 1024 // 4KB

// table of contents
type IndexEntry struct {
	key    string
	Offset int64
}

type SSTableBuilder struct {
	file          *os.File
	index         []IndexEntry
	filter        *bloom.BloomFilter
	currentOffset int64
	blockStart    int64
}

func NewSSTableBuilder(filename string, keyCount int) (*SSTableBuilder, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	bf := bloom.New(keyCount)

	return &SSTableBuilder{
		file:          f,
		filter:        bf,
		currentOffset: 0,
		blockStart:    0,
	}, nil
}

func createSSTable(frozenMem *MemTable, walDir string) error { // Added walDir arg for cleaner path handling
	// sort
	keys := make([]string, 0, len(frozenMem.Index))
	for k := range frozenMem.Index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filename := fmt.Sprintf("%s/level0_%d.sst", walDir, time.Now().UnixNano()) // Use walDir path
	builder, err := NewSSTableBuilder(filename, len(keys))
	if err != nil {
		return fmt.Errorf("failed to create sstable file: %w", err)
	}

	// add to builder
	for _, k := range keys {
		offset := frozenMem.Index[k]
		val, isTombstone, _ := frozenMem.Arena.Get(offset)

		// Convert string to []byte for the Builder
		err := builder.Add([]byte(k), val, isTombstone)
		if err != nil {
			// If write fails, we should probably close and delete the corrupt file
			_ = builder.file.Close()
			_ = os.Remove(filename)
			return fmt.Errorf("failed to add key to sstable: %w", err)
		}
	}

	if err := builder.Close(); err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}

	// 5. delete wal
	if frozenMem.Wal != nil {
		if err := frozenMem.Wal.Remove(); err != nil {
			fmt.Printf("Warning: failed to delete old WAL: %v\n", err)
		}
	}

	return nil
}

func (b *SSTableBuilder) Add(key []byte, val []byte, isTombstone bool) error {
	//Sparse Index: logic
	b.filter.Add(key)
	if b.currentOffset == 0 || (b.currentOffset-b.blockStart) > blockSize {
		b.index = append(b.index, IndexEntry{
			key:    string(key),
			Offset: b.currentOffset,
		})
		b.blockStart = b.currentOffset
	}
	header := byte(0)
	if isTombstone {
		header = 1
	}
	if _, err := b.file.Write([]byte{header}); err != nil {
		return err
	}
	var lenBuf [6]byte
	binary.LittleEndian.PutUint16(lenBuf[0:2], uint16(len(key)))
	valLen := len(val)
	if isTombstone {
		valLen = 0 // Tombstones have no val
	}
	binary.LittleEndian.PutUint32(lenBuf[2:6], uint32(valLen))
	if _, err := b.file.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := b.file.Write(key); err != nil {
		return err
	}
	if !isTombstone {
		if _, err := b.file.Write(val); err != nil {
			return err
		}
	}
	// Size = Header(1) + KeyLen(2) + ValLen(4) + KeyBytes + ValBytes
	entrySize := int64(1 + 2 + 4 + len(key) + valLen)
	b.currentOffset += entrySize

	return nil
}

func (b *SSTableBuilder) Close() error {
	indexStartOffset := b.currentOffset // end of the last block is the start of the index entries
	// Format for each entry: [KeyLen(2)][KeyBytes][Offset(8)]
	var buf [10]byte // Reusable buffer for lengths and offset
	for _, entry := range b.index {
		binary.LittleEndian.PutUint16(buf[0:2], uint16(len(entry.key)))
		if _, err := b.file.Write(buf[0:2]); err != nil {
			return err
		}

		if _, err := b.file.WriteString(entry.key); err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(buf[2:10], uint64(entry.Offset))
		if _, err := b.file.Write(buf[2:10]); err != nil {
			return err
		}
		entrySize := int64(2 + len(entry.key) + 8)
		b.currentOffset += entrySize
	}
	filterStartOffset := b.currentOffset // This is where the filter begins
	filterBytes := b.filter.Bytes()
	if _, err := b.file.Write(filterBytes); err != nil {
		return err
	}
	b.currentOffset += int64(len(filterBytes))

	//Write the footer
	// [Index Offset (8 bytes)] + [Filter Offset (8 bytes)]
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexStartOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(filterStartOffset))

	if _, err := b.file.Write(footer[:]); err != nil {
		return err
	}

	if err := b.file.Sync(); err != nil {
		return err
	}
	return b.file.Close()
}
