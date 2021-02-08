package cmd

type FileRecord struct {
	ChunkSize uint32
	FileSize  uint64
}

type ChunkRecord struct {
	start uint64
	length uint32
	index uint32
}

func GetChunksForFile(record FileRecord) []ChunkRecord {
	if record.FileSize == 0 {
		return []ChunkRecord{}
	}
	recordCount := ((record.FileSize - 1) / uint64(record.ChunkSize)) + 1
	records := make([]ChunkRecord, recordCount)
	for i := uint64(0); i < recordCount - 1; i++ {
		records[i] = ChunkRecord{
			start:  i * uint64(record.ChunkSize),
			length: record.ChunkSize,
			index:  uint32(i),
		}
	}
	start := (recordCount-1) * uint64(record.ChunkSize)
	records[recordCount-1] = ChunkRecord{
		start:  start,
		length: uint32(record.FileSize - start),
		index: uint32(recordCount),
	}
	return records
}