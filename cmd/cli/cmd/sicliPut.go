package cmd

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Put a key-value pair into the KV store",
	Long:  `Store a key-value pair in the KV store.`,
	Example: `  sicli put mykey myvalue
  sicli put mykey myvalue --addr http://localhost:8081
  sicli put "my key" "my value"`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		value := args[1]
		
		requestURL := fmt.Sprintf(
			"%s/put?key=%s&val=%s",
			baseURL,
			url.QueryEscape(key),
			url.QueryEscape(value),
		)

		_, err := doRequest("POST", requestURL)
		if err != nil {
			return err
		}

		fmt.Println("---Success---")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(putCmd)
}
