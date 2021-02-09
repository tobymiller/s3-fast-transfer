package cmd

import (
	json2 "encoding/json"
	"errors"
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
		json, err := downloadJson(s3Abstract)
		if err != nil {
			panic(err)
		}
		var record FileRecord
		err = json2.Unmarshal(json, &record)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(record)
		RunThreads(downloadPart, chunks, downloadOpenFile, int(threadCount))
		err = os.Truncate(downloadInput, int64(record.FileSize))
		// Ignore error, which happens when the file isn't a regular file.
		// We may have written past the end, to the nearest aligned block, but there's nothing we can do to fix that.
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
	stat, err := os.Stat(downloadInput)
	if err != nil {
		return nil, err, nil
	}
	var file *os.File
	if os.IsNotExist(err) {
		file, err = os.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_CREAT, 0666)
	} else if err != nil {
		return nil, err, nil
	} else {
		if stat.Mode().IsDir() {
			return nil, errors.New("output is directory"), nil
		} else if stat.Mode().IsRegular() {
			file, err = os.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_TRUNC, 0666)
		} else { // assume it's a block device for now
			file, err = directio.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_SYNC, 0666)
		}
	}
	block := directio.AlignedBlock(directio.BlockSize)
	return FileAndBuffer{
		file,
		block,
	}, err, file.Close
}

func init() {
	rootCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().StringVar(&downloadInput, "output", "", "Output file path")
	downloadCmd.Flags().StringVar(&downloadKey, "key", "", "S3 Key")
}
