package cmd

import (
	"github.com/ncw/directio"
	"os"
	"syscall"

	"github.com/spf13/cobra"
)

var downloadInput string
var downloadKey string

var fileSize uint64

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a local file from S3",
	Run: func(cmd *cobra.Command, args []string) {
		setupS3Client()
		s3Abstract = S3AbstractLocation{
			bucket:     bucket,
			filePrefix: downloadKey,
		}
		record := FileRecord{
			ChunkSize: chunkSize,
			FileSize:  fileSize,
		}
		chunks := GetChunksForFile(record)
		RunThreads(downloadPart, chunks, downloadOpenFile, int(threadCount))
	},
}

type FileAndBuffer struct  {
	file *os.File
	buffer []byte
}

func downloadPart(chunk interface{}, fileAndBuffer interface{}) (interface{}, error) {
	file := fileAndBuffer.(FileAndBuffer).file
	buffer := fileAndBuffer.(FileAndBuffer).buffer
	err := download(s3Abstract, chunk.(ChunkRecord), *file, buffer)
	return 0, err
}

func downloadOpenFile() (interface{}, error, func() error) {
	file, err := directio.OpenFile(downloadInput, syscall.O_RDWR|syscall.O_CREAT, 0666)
	block := directio.AlignedBlock(directio.BlockSize)
	return FileAndBuffer{
		file,
		block,
	}, err, file.Close
}

func init() {
	rootCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().Uint32Var(&chunkSize, "chunkSize", 1024 * 1024, "Chunk size in bytes")
	downloadCmd.Flags().Uint64Var(&fileSize, "fileSize", 0, "File size in bytes")
	downloadCmd.Flags().StringVar(&downloadInput, "input", "", "Output file path")
	downloadCmd.Flags().StringVar(&downloadKey, "key", "", "S3 Key")
}
