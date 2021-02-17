package cmd

import (
	json2 "encoding/json"
	"errors"
	"fmt"
	"github.com/ncw/directio"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var downloadInput string
var downloadKey string

var fileSize uint64
var threadCount uint8
var drop bool

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
		start := time.Now()
		RunThreads(downloadPart, chunks, downloadOpenFile, int(threadCount))
		timeTotal := time.Since(start)
		speed := float64(record.FileSize) / timeTotal.Seconds()
		fmt.Printf("Download completed in %d ms\n", timeTotal.Milliseconds())
		fmt.Printf("Bytes per second: %d\n", int64(speed))
	},
}

type FileAndBuffer struct  {
	file *os.File
	buffer []byte
	direct bool
}

func downloadPart(chunk interface{}, fileAndBuffer interface{}) (interface{}, error) {
	file := fileAndBuffer.(FileAndBuffer).file
	buffer := fileAndBuffer.(FileAndBuffer).buffer
	directIo := fileAndBuffer.(FileAndBuffer).direct
	err := download(s3Abstract, chunk.(ChunkRecord), file, buffer, directIo, drop)
	return 0, err
}

func downloadOpenFile() (interface{}, error, func() error) {
	if drop {
		return FileAndBuffer{
			file:   nil,
			buffer: make([]byte, blockSize),
			direct: false,
		}, nil, func() error { return nil }
	}
	stat, err := os.Stat(downloadInput)
	var file *os.File
	directBlock := false
	if os.IsNotExist(err) {
		file, err = os.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_CREAT, 0666)
	} else if err != nil {
		return nil, err, nil
	} else {
		if stat.Mode().IsDir() {
			return nil, errors.New("output is directory"), nil
		} else if stat.Mode().IsRegular() || noDirectIo {
			file, err = os.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_TRUNC, 0666)
		} else { // assume it's a block device for now
			file, err = directio.OpenFile(downloadInput, syscall.O_WRONLY|syscall.O_SYNC, 0666)
			directBlock = true
		}
	}
	var block []byte
	if directBlock {
		block = directio.AlignedBlock(directio.BlockSize)
	} else {
		block = make([]byte, blockSize)
	}
	return FileAndBuffer{
		file,
		block,
		directBlock,
	}, err, file.Close
}

func init() {
	rootCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().StringVar(&downloadInput, "output", "", "Output file path")
	downloadCmd.Flags().StringVar(&downloadKey, "key", "", "S3 Key")
	downloadCmd.Flags().BoolVar(&drop, "drop", false, "Drop all data once downloaded - used for testing network speed in isolation")
	downloadCmd.PersistentFlags().Uint8Var(&threadCount, "threadCount", 8, "Number of parallel streams to S3")
}
