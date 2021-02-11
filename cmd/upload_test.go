package cmd

import (
	"testing"
)

func TestCalculateChunkSizeForFileAndThreads(t *testing.T) {
	tests := []struct {
		fileSize uint64
		threadCount uint8
		expectedChunkSize uint32
	}{
		{
			fileSize:          1024 * 1024 * 1024 * 1024,
			threadCount:       64 * 3,
			expectedChunkSize: 4096 * 174762,
		},
		{
			fileSize:          1024 * 1024 * 1024 * 1024 + 12345678,
			threadCount:       64 * 3,
			expectedChunkSize: 4096 * 174764,
		},
		{
			fileSize:          1024 * 1024 * 1024 * 1024 - 12345678,
			threadCount:       64 * 3,
			expectedChunkSize: 4096 * 174760,
		},
		{
			fileSize:          1024 * 1024 * 1024,
			threadCount:       64 * 3,
			expectedChunkSize: 1024 * 1024 * 32,
		},
		{
			fileSize:          1024 * 1024 * 1024,
			threadCount:       1,
			expectedChunkSize: 1024 * 1024 * 128,
		},
		{
			fileSize:          1024 * 1024 * 1024 * 1024 * 8,
			threadCount:       1,
			expectedChunkSize: 1024 * 1024 * 1024 * 2,
		},
	}

	for _, test := range tests {
		result := CalculateChunkSizeForFileAndThreads(test.fileSize, test.threadCount)
		if result != test.expectedChunkSize {
			t.Errorf("Failure in chunk size calculation test. Expected %d, got %d", test.expectedChunkSize, result)
		}
	}
}
