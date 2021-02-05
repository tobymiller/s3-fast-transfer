package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a local file to S3",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("upload called")
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
	// uploadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
