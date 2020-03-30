package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cete",
		Short: "The lightweight distributed key value store server",
		Long:  "The lightweight distributed key value store server",
	}
)

func Execute() error {
	return rootCmd.Execute()
}
