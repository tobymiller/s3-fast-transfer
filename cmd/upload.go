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

var uploadThreadCount uint8
var uploadTargetThreadCount uint8

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
		chunkSize := CalculateChunkSizeForFileAndThreads(length, uploadTargetThreadCount)
		record := FileRecord{
			ChunkSize: chunkSize,
			FileSize:  length,
		}
		recordJson, err := json2.Marshal(record)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(record)
		RunThreads(uploadPart, chunks, uploadOpenFile, int(uploadThreadCount))
		uploadJson(s3Abstract, recordJson)
	},
}

func CalculateChunkSizeForFileAndThreads(fileSize uint64, threadCount uint8) uint32 {
	const m32 = 1024 * 1024 * 32
	const g2 = 1024 * 1024 * 1024 * 2
	// It's important that this is a multiple of all the direct io block sizes for different platforms.
	// Fortunately they're all 4096, but I'm making this a constant so that users can't accidentally set it to something that isn't aligned.
	idealChunkSize := fileSize / (uint64(threadCount) * 8)
	toNearest4096 := (idealChunkSize / 4096) * 4096
	asInt := uint32(toNearest4096)
	if toNearest4096 > g2 { // 2GB (bigger would cause maxint32 problems)
		return g2
	} else if asInt < m32 {
		return m32
	} else {
		return asInt
	}
}

func uploadPart(chunk interface{}, file interface{}) (interface{}, error) {
	err := upload(s3Abstract, chunk.(ChunkRecord), file.(*os.File))
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
	uploadCmd.PersistentFlags().Uint8Var(&uploadThreadCount, "threadCount", 8, "Number of parallel streams to S3")
	uploadCmd.PersistentFlags().Uint8Var(&uploadTargetThreadCount, "targetThreadCount", 8, "Number of parallel streams to S3 to aim for for later download")
}
