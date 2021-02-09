package cmd

import (
	"os"
	"syscall"

	"github.com/spf13/cobra"
)

var downloadInput string
var downloadKey string

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a local file from S3",
	Run: func(cmd *cobra.Command, args []string) {
		setupS3Client()
		s3Abstract = S3AbstractLocation{
			bucket:     bucket,
			filePrefix: downloadKey,
		}
		record, err := GetFileRecordForPath(downloadInput, chunkSize)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(record)
		RunThreads(downloadPart, chunks, downloadOpenFile, int(threadCount))
	},
}

func downloadPart(chunk interface{}, file interface{}) (interface{}, error) {
	err := download(s3Abstract, chunk.(ChunkRecord), *file.(*os.File))
	return 0, err
}

func downloadOpenFile() (interface{}, error, func() error) {
	file, err := os.OpenFile(downloadInput, syscall.O_RDWR|syscall.O_CREAT, 0666)
	return file, err, file.Close
}

func init() {
	rootCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().Uint32Var(&chunkSize, "chunkSize", 1024 * 1024, "Chunk size in bytes")
	downloadCmd.Flags().StringVar(&downloadInput, "input", "", "Output file path")
	downloadCmd.Flags().StringVar(&downloadKey, "key", "", "S3 Key")
}
