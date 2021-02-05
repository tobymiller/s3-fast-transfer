package cmd

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/http"
	"os"
	"time"
)

type S3AbstractLocation struct {
	bucket string
	filePrefix string
}

type S3Location struct {
	bucket string
	key string
}

func (l* S3AbstractLocation) GetChunkLocation(chunk ChunkRecord) S3Location {
	return S3Location{
		bucket: l.bucket,
		key:    fmt.Sprintf("%s/%d", l.filePrefix, chunk.index),
	}
}

var bufferLength = 1024 * 1024

var s3Client *s3.Client
var s3Context = context.Background()

func setupS3Client() {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := config.LoadDefaultConfig(s3Context)
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	// cfg.Region = region

	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Second * 180,
	}

	s3Client = s3.NewFromConfig(cfg)
}

func download(abstractLocation S3AbstractLocation, chunk ChunkRecord, outFile os.File) {
	location := abstractLocation.GetChunkLocation(chunk)
	resp, err := s3Client.GetObject(s3Context, &s3.GetObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
	})
	if err != nil {
		panic("Failed to get object: " + err.Error())
	}
	_, err = outFile.Seek(int64(chunk.start),0)
	if err != nil {
		panic("Failed to seek for writing: " + err.Error())
	}
	var buf = make([]byte, bufferLength)
	for {
		n, err := resp.Body.Read(buf)
		_, err2 := outFile.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			panic("Error reading object body: " + err.Error())
		}
		if err2 != nil {
			panic("Error reading object body: " + err2.Error())
		}
	}
	_ = resp.Body.Close()
}

type S3ReaderFunc func([]byte) (int, error)

func (r S3ReaderFunc) Read(b []byte) (int, error) {
	return r(b)
}

func upload(abstractLocation S3AbstractLocation, chunk ChunkRecord, inFile os.File) {
	location := abstractLocation.GetChunkLocation(chunk)
	_, err := inFile.Seek(int64(chunk.start),0)
	if err != nil {
		panic("Failed to seek for reading: " + err.Error())
	}
	bytesLeft := chunk.length
	_, err = s3Client.PutObject(s3Context, &s3.PutObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
		Body:   S3ReaderFunc(func(b []byte) (int, error) {
			bb := b
			if uint32(len(b)) > bytesLeft {
				bb = b[:bytesLeft]
			}
			n, e := inFile.Read(bb)
			bytesLeft -= uint32(n)
			if bytesLeft == 0 {
				e = io.EOF
			}
			return n, e
		}),
	})
	if err != nil {
		panic("Failed to put object: " + err.Error())
	}
}