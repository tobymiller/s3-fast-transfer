package cmd

import (
	"bytes"
	"context"
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

func download(abstractLocation S3AbstractLocation, chunk ChunkRecord, outFile os.File) error {
	location := abstractLocation.GetChunkLocation(chunk)
	resp, err := s3Client.GetObject(s3Context, &s3.GetObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
	})
	if err != nil {
		return err
	}
	_, err = outFile.Seek(int64(chunk.start),0)
	if err != nil {
		return err
	}
	var buf = make([]byte, bufferLength)
	for {
		n, err := resp.Body.Read(buf)
		_, err2 := outFile.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err2 != nil {
			return err
		}
	}
	_ = resp.Body.Close()
	return nil
}

func upload(abstractLocation S3AbstractLocation, chunk ChunkRecord, inFile os.File) error {
	location := abstractLocation.GetChunkLocation(chunk)
	_, err := inFile.Seek(int64(chunk.start),0)
	if err != nil {
		return err
	}
	var buf = make([]byte, bufferLength)
	n, e := inFile.Read(buf)
	if e != nil {
		return err
	}
	if n != bufferLength {
		return errors.New("did not read whole buffer")
	}
	_, err = s3Client.PutObject(s3Context, &s3.PutObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.key),
		Body:   bytes.NewReader(buf),
	})
	return err
}