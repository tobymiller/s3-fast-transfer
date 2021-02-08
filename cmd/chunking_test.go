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
			ChunkSize: 1,
			FileSize:  1,
		}, []ChunkRecord{
			{
				start:  0,
				length: 1,
			},
		}},
		{FileRecord{
			ChunkSize: 2,
			FileSize:  1,
		}, []ChunkRecord{
			{
				start:  0,
				length: 1,
			},
		}},
		{FileRecord{
			ChunkSize: 1,
			FileSize:  0,
		}, []ChunkRecord{}},
		{FileRecord{
			ChunkSize: 2,
			FileSize:  3,
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
			ChunkSize: 2,
			FileSize:  4,
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
