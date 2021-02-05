package cmd

import (
	"reflect"
	"testing"
)

func TestGetChunksForFile(t *testing.T) {
	tests := []struct {
		input          FileRecord
		expectedOutput []ChunkRecord
	}{
		{FileRecord{
			chunkSize: 1,
			fileSize:  1,
		}, []ChunkRecord{
			{
				start:  0,
				length: 1,
			},
		}},
		{FileRecord{
			chunkSize: 2,
			fileSize:  1,
		}, []ChunkRecord{
			{
				start:  0,
				length: 1,
			},
		}},
		{FileRecord{
			chunkSize: 1,
			fileSize:  0,
		}, []ChunkRecord{}},
		{FileRecord{
			chunkSize: 2,
			fileSize:  3,
		}, []ChunkRecord{
			{
				start:  0,
				length: 2,
			},
			{
				start:  2,
				length: 1,
			},
		}},
		{FileRecord{
			chunkSize: 2,
			fileSize:  4,
		}, []ChunkRecord{
			{
				start:  0,
				length: 2,
			},
			{
				start:  2,
				length: 2,
			},
		}},
	}

	for _, test := range tests {
		result := GetChunksForFile(test.input)
		if !reflect.DeepEqual(result, test.expectedOutput) {
			t.Errorf("Failure in chunking test")
		}
	}
}
