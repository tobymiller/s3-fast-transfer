package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"io/ioutil"
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

func (l* S3AbstractLocation) GetJsonLocation() S3Location {
	return S3Location{
		bucket: l.bucket,
		key:    fmt.Sprintf("%s/manifest.json", l.filePrefix),
	}
}

var bufferLength = 1024 * 1024

var s3Client *s3.Client
var s3Context = context.Background()

func getRegion() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/placement/availability-zone"
	response, err := httpClient.Get(link)
	if err != nil {
		panic(err)
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	az := string(content)

	return az[:len(az)-1]
}

func setupS3Client() {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := config.LoadDefaultConfig(s3Context)
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	cfg.Region = getRegion()

	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Second * 180,
	}

	s3Client = s3.NewFromConfig(cfg)
}

func download(abstractLocation S3AbstractLocation, chunk ChunkRecord, outFile *os.File, buf []byte, directIo bool, drop bool) error {
	for {
		location := abstractLocation.GetChunkLocation(chunk)
		resp, err := s3Client.GetObject(s3Context, &s3.GetObjectInput{
			Bucket: aws.String(location.bucket),
			Key:    aws.String(location.key),
		})
		if err != nil {
			return err
		}
		if !drop {
			_, err = outFile.Seek(int64(chunk.start), 0)
			if err != nil {
				return err
			}
		}
		total := 0
		expectedMd5 := *resp.ETag
		md5Builder := md5.New()
		for {
			n, err := io.ReadFull(resp.Body, buf)
			var err2 error
			if !drop {
				if directIo {
					_, err2 = outFile.Write(buf)
				} else {
					_, err2 = outFile.Write(buf[:n])
				}
				md5Builder.Write(buf[:n])
			}
			total += n
			if err == io.EOF || err == io.ErrUnexpectedEOF || uint32(total) >= chunk.length {
				break
			}
			if err != nil {
				return err
			}
			if err2 != nil {
				return err2
			}
		}
		_ = resp.Body.Close()
		if !drop {
			actualMd5 := fmt.Sprintf("\"%x\"", md5Builder.Sum(nil))
			if expectedMd5 != actualMd5 {
				println(fmt.Sprintf("Md5 for block didn't match - will retry. Expected: %s, actual %s, index %d", expectedMd5, actualMd5, chunk.index))
			} else {
				break
			}
		} else {
			break
		}
	}
	return nil
}

type S3ReaderFunc struct {
	read func([]byte) (int, error)
	seek func(offset int64, whence int) (int64, error)
}

func (r S3ReaderFunc) Read(b []byte) (int, error) {
	return r.read(b)
}

func (r S3ReaderFunc) Seek(offset int64, whence int) (int64, error) {
	return r.seek(offset, whence)
}

func upload(abstractLocation S3AbstractLocation, chunk ChunkRecord, inFile *os.File) error {
	location := abstractLocation.GetChunkLocation(chunk)
	bytesLeft := uint32(0)
	reader := S3ReaderFunc{
		read: func(b []byte) (int, error) {
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
		},
		seek: func(offset int64, whence int) (int64, error) {
			if whence == 2 {
				if offset == 0 {
					_, err := inFile.Seek(int64(chunk.start+uint64(chunk.length)), 0)
					bytesLeft = 0
					return int64(chunk.length), err
				}
			} else if whence == 1 {
				if offset == 0 {
					n, err := inFile.Seek(0, 1)
					return n - int64(chunk.start), err
				}
			} else if whence == 0 {
				if offset == 0 {
					_, err := inFile.Seek(int64(chunk.start), 0)
					bytesLeft = chunk.length
					return 0, err
				}
			}
			return 0, errors.New(fmt.Sprintf("should only seek to beginning, not offset %d, whence %d", offset, whence))
		},
	}
	_, err := reader.Seek(0,0)
	if err != nil {
		return err
	}
	_, err = s3Client.PutObject(s3Context, &s3.PutObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
		Body:   reader,
	})
	return err
}

func uploadJson(abstractLocation S3AbstractLocation, json []byte) error {
	location := abstractLocation.GetJsonLocation()
	_, err := s3Client.PutObject(s3Context, &s3.PutObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
		Body:   bytes.NewReader(json),
	})
	return err
}

func downloadJson(abstractLocation S3AbstractLocation) ([]byte, error) {
	location := abstractLocation.GetJsonLocation()
	res, err := s3Client.GetObject(s3Context, &s3.GetObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
	})
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(res.Body)
}