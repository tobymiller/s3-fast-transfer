package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var bucket string
var noDirectIo bool
var blockSize uint32
var retries uint8

var rootCmd = &cobra.Command{
	Use:   "s3-fast-transfer",
	Short: "Maximise throughput to S3",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&bucket, "bucket", "", "S3 bucket")
	rootCmd.PersistentFlags().BoolVar(&noDirectIo, "no-direct-io", false, "Use normal io even on block devices")
	rootCmd.PersistentFlags().Uint32Var(&blockSize, "block-size", 1024 * 1024 * 8, "For non-direct io only, what block size to use")
	rootCmd.PersistentFlags().Uint8Var(&retries, "retries", 3, "Number of retry attempts to make for each call to S3")
}
