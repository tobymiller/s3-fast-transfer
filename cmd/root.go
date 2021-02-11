package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var bucket string

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
}
