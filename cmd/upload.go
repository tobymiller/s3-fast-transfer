package cmd

import (
	json2 "encoding/json"
	"os"
	"syscall"

	"github.com/spf13/cobra"
)

var uploadInput string
var uploadKey string
var s3Abstract S3AbstractLocation

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a local file to S3",
	Run: func(cmd *cobra.Command, args []string) {
		setupS3Client()
		s3Abstract = S3AbstractLocation{
			bucket:     bucket,
			filePrefix: uploadKey,
		}
		length, err := GetFileSizeForPath(uploadInput)
		if err != nil {
			panic(err)
		}
		chunkSize := CalculateChunkSizeForFileAndThreads(length, threadCount)
		record := FileRecord{
			ChunkSize: chunkSize,
			FileSize:  length,
		}
		recordJson, err := json2.Marshal(record)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(record)
		RunThreads(uploadPart, chunks, uploadOpenFile, int(threadCount))
		uploadJson(s3Abstract, recordJson)
	},
}

func CalculateChunkSizeForFileAndThreads(fileSize uint64, threadCount uint8) uint32 {
	const m32 = 1024 * 1024 * 32
	// It's important that this is a multiple of all the direct io block sizes for different platforms.
	// Fortunately they're all 4096, but I'm making this a constant so that users can't accidentally set it to something that isn't aligned.
	idealChunkSize := fileSize / (uint64(threadCount) * 8)
	toNearest4096 := uint32((idealChunkSize / 4096) * 4096)
	if toNearest4096 < m32 {
		return m32
	} else {
		return toNearest4096
	}
}

func uploadPart(chunk interface{}, file interface{}) (interface{}, error) {
	err := upload(s3Abstract, chunk.(ChunkRecord), *file.(*os.File))
	return 0, err
}

func uploadOpenFile() (interface{}, error, func() error) {
	file, err := os.OpenFile(uploadInput, syscall.O_RDONLY, 0)
	return file, err, file.Close
}

func init() {
	rootCmd.AddCommand(uploadCmd)
	uploadCmd.Flags().StringVar(&uploadInput, "input", "", "Input file path")
	uploadCmd.Flags().StringVar(&uploadKey, "key", "", "S3 Key")
}
