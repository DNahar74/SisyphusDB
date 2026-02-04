package cmd

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Get a value from the KV store",
	Long:  `Retrieve a value from the KV store by its key.`,
	Example: `  sicli get mykey
  sicli get mykey --addr http://localhost:8081`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		requestURL := fmt.Sprintf("%s/get?key=%s", baseURL, url.QueryEscape(key))

		val, err := doRequest("GET", requestURL)
		if err != nil {
			return err
		}

		fmt.Println(val)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
