package cmd

type FileRecord struct {
	chunkSize uint32
	fileSize uint64
}

type ChunkRecord struct {
	start uint64
	length uint32
	index uint32
}

func GetChunksForFile(record FileRecord) []ChunkRecord {
	if record.fileSize == 0 {
		return []ChunkRecord{}
	}
	recordCount := ((record.fileSize - 1) / uint64(record.chunkSize)) + 1
	records := make([]ChunkRecord, recordCount)
	for i := uint64(0); i < recordCount - 1; i++ {
		records[i] = ChunkRecord{
			start:  i * uint64(record.chunkSize),
			length: record.chunkSize,
			index:  uint32(i),
		}
	}
	start := (recordCount-1) * uint64(record.chunkSize)
	records[recordCount-1] = ChunkRecord{
		start:  start,
		length: uint32(record.fileSize - start),
		index: uint32(recordCount),
	}
	return records
}