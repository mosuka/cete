package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/protobuf"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	deleteCmd = &cobra.Command{
		Use:   "delete KEY",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a key-value",
		Long:  "Delete a key-value",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress = viper.GetString("grpc_address")

			certificateFile = viper.GetString("certificate_file")
			commonName = viper.GetString("common_name")

			key := args[0]

			c, err := client.NewGRPCClientWithContextTLS(grpcAddress, context.Background(), certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.DeleteRequest{
				Key: key,
			}

			if err := c.Delete(req); err != nil {
				return err
			}

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(deleteCmd)

	cobra.OnInitialize(func() {
		if configFile != "" {
			viper.SetConfigFile(configFile)
		} else {
			home, err := homedir.Dir()
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			viper.AddConfigPath("/etc")
			viper.AddConfigPath(home)
			viper.SetConfigName("cete")

		}

		viper.SetEnvPrefix("CETE")
		viper.AutomaticEnv()

		if err := viper.ReadInConfig(); err != nil {
			switch err.(type) {
			case viper.ConfigFileNotFoundError:
				// cete.yaml does not found in config search path
			default:
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	})

	deleteCmd.PersistentFlags().StringVar(&configFile, "config-file", "", "config file. if omitted, cete.yaml in /etc and home directory will be searched")
	deleteCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listen address")
	deleteCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	deleteCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	_ = viper.BindPFlag("grpc_address", deleteCmd.PersistentFlags().Lookup("grpc-address"))
	_ = viper.BindPFlag("certificate_file", deleteCmd.PersistentFlags().Lookup("certificate-file"))
	_ = viper.BindPFlag("common_name", deleteCmd.PersistentFlags().Lookup("common-name"))
}
