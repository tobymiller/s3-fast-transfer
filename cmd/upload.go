package cmd

import (
	"os"
	"syscall"

	"github.com/spf13/cobra"
)

var uploadInput string
var uploadKey string
var s3Abstract S3AbstractLocation
var chunkSize uint32

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a local file to S3",
	Run: func(cmd *cobra.Command, args []string) {
		setupS3Client()
		s3Abstract = S3AbstractLocation{
			bucket:     bucket,
			filePrefix: uploadKey,
		}
		record, err := GetFileRecordForPath(uploadInput, chunkSize)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(record)
		RunThreads(uploadPart, chunks, uploadOpenFile, int(threadCount))
	},
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
	uploadCmd.Flags().Uint32Var(&chunkSize, "chunkSize", 1024 * 1024, "Chunk size in bytes")
	uploadCmd.Flags().StringVar(&uploadInput, "input", "", "Input file path")
	uploadCmd.Flags().StringVar(&uploadKey, "key", "", "S3 Key")
}
