package cmd

import (
	"reflect"
	"testing"
)

func TestGetChunksForFile(t *testing.T) {
	tests := []struct {
		input          FileRecord
		expectedOutput []interface{}
	}{
		{FileRecord{
			ChunkSize: 1,
			FileSize:  1,
		}, []interface{}{
			ChunkRecord{
				start:  0,
				length: 1,
				index:  0,
			},
		}},
		{FileRecord{
			ChunkSize: 2,
			FileSize:  1,
		}, []interface{}{
			ChunkRecord{
				start:  0,
				length: 1,
				index:  0,
			},
		}},
		{FileRecord{
			ChunkSize: 1,
			FileSize:  0,
		}, []interface{}{}},
		{FileRecord{
			ChunkSize: 2,
			FileSize:  3,
		}, []interface{}{
			ChunkRecord{
				start:  0,
				length: 2,
				index:  0,
			},
			ChunkRecord{
				start:  2,
				length: 1,
				index:  1,
			},
		}},
		{FileRecord{
			ChunkSize: 2,
			FileSize:  4,
		}, []interface{}{
			ChunkRecord{
				start:  0,
				length: 2,
				index:  0,
			},
			ChunkRecord{
				start:  2,
				length: 2,
				index:  1,
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
