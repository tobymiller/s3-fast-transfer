package cmd

import (
	json2 "encoding/json"
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
var numberOfDirFiles uint8
var drop bool
var dir bool

var fileRecord FileRecord
var chunkMod uint64
var chunksPerFile uint64

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
		err = json2.Unmarshal(json, &fileRecord)
		if err != nil {
			panic(err)
		}
		chunks := GetChunksForFile(fileRecord)
		UpdateNumberOfDirFiles(chunks)
		start := time.Now()
		RunThreads(downloadPart, chunks, downloadOpenFile, int(threadCount))
		timeTotal := time.Since(start)
		speed := float64(fileRecord.FileSize) / timeTotal.Seconds()
		fmt.Printf("Download completed in %d ms\n", timeTotal.Milliseconds())
		fmt.Printf("Bytes per second: %d\n", int64(speed))
	},
}

func UpdateNumberOfDirFiles(chunks []interface{}) {
	numberOfChunks := len(chunks)
	if numberOfChunks == 0 {
		numberOfDirFiles = 0
		chunkMod = 1
	}
	maxChunkSize := chunks[0].(ChunkRecord).length
	for numberOfChunks > int(numberOfDirFiles) {
		numberOfChunks = (numberOfChunks+1) / 2
	}
	numberOfDirFiles = uint8(numberOfChunks)
	chunksPerFile = uint64(len(chunks)) / uint64(numberOfDirFiles)
	chunkMod = uint64(maxChunkSize) * chunksPerFile

}

type FilesAndBuffer struct  {
	file func(chunk ChunkRecord) (*os.File, ChunkRecord, error)
	buffer []byte
	direct bool
}

func downloadPart(chunk interface{}, fileAndBuffer interface{}) (interface{}, error) {
	file := fileAndBuffer.(FilesAndBuffer).file
	buffer := fileAndBuffer.(FilesAndBuffer).buffer
	directIo := fileAndBuffer.(FilesAndBuffer).direct
	f, newChunk, err := file(chunk.(ChunkRecord))
	if err != nil {
		return 0, err
	}
	err = download(s3Abstract, newChunk, f, buffer, directIo, drop)
	return 0, err
}

type Method int
const (
	regular Method = iota
	blockDevice
	directory
)

func downloadOpenFile() (interface{}, error, func() error) {
	if drop {
		return FilesAndBuffer{
			file:   func(chunk ChunkRecord) (*os.File, ChunkRecord, error) { return nil, chunk, nil },
			buffer: make([]byte, blockSize),
			direct: false,
		}, nil, func() error { return nil }
	}
	stat, err := os.Stat(downloadInput)
	var method Method
	if os.IsNotExist(err) {
		if dir {
			method = directory
		} else {
			method = regular
		}
	} else if err != nil {
		return nil, err, func() error { return nil }
	} else {
		if stat.Mode().IsDir() {
			method = directory
		} else if stat.Mode().IsRegular() || noDirectIo {
			method = regular
		} else { // assume it's a block device for now
			method = blockDevice
		}
	}
	if method == directory {
		_ = os.Mkdir(downloadInput, 0644)
		// error will just be directory existing, which is fine
	} else if method == regular {
		_ = os.Truncate(downloadInput, 0)
		// error will just be if it doesn't exist, which is fine
	}
	var files = make([]*os.File, numberOfDirFiles)
	var getOrOpenFileCall = func (number uint32) (*os.File, error) {
		var existingFile = files[number]
		if existingFile != nil {
			return existingFile, nil
		}

		var file *os.File
		var err error
		if method == regular {
			file, err = os.OpenFile(downloadInput, syscall.O_CREAT | syscall.O_WRONLY, 0666)
		} else if method == blockDevice {
			file, err = directio.OpenFile(downloadInput, syscall.O_SYNC | syscall.O_WRONLY, 0666)
		} else if method == directory {
			file, err = os.OpenFile(fmt.Sprintf("%s/%d", downloadInput, number), syscall.O_CREAT | syscall.O_WRONLY, 0666)
		}
		files[number] = file
		return file, err
	}
	var block []byte
	if method == blockDevice {
		block = directio.AlignedBlock(directio.BlockSize)
	} else {
		block = make([]byte, blockSize)
	}
	return FilesAndBuffer{
		func(chunk ChunkRecord) (*os.File, ChunkRecord, error) {
			if method != directory {
				f, err := getOrOpenFileCall(0)
				return f, chunk, err
			}

			f, err := getOrOpenFileCall(chunk.index / uint32(chunksPerFile))
			return f, ChunkRecord{
				start:  chunk.start % chunkMod,
				length: chunk.length,
				index:  chunk.index,
			}, err
		},
		block,
		method == blockDevice,
	}, err, func () error {
		for _, file := range files {
			if file != nil {
				err := file.Close()
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func init() {
	rootCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().StringVar(&downloadInput, "output", "", "Output file path")
	downloadCmd.Flags().StringVar(&downloadKey, "key", "", "S3 Key")
	downloadCmd.Flags().BoolVar(&drop, "drop", false, "Drop all data once downloaded - used for testing network speed in isolation")
	downloadCmd.Flags().BoolVar(&dir, "dir", false, "Download as collection of files")
	downloadCmd.PersistentFlags().Uint8Var(&threadCount, "threadCount", 8, "Number of parallel streams to S3")
	downloadCmd.PersistentFlags().Uint8Var(&numberOfDirFiles, "numberOfDirFiled", 8, "Number of files to use in directory mode")
}
