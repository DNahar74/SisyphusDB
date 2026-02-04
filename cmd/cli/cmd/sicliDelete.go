package cmd

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete a key from the KV store",
	Long:  `Remove a key and its associated value from the KV store.`,
	Example: `  sicli delete mykey
  sicli delete mykey --addr http://localhost:8081`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		requestURL := fmt.Sprintf("%s/delete?key=%s", baseURL, url.QueryEscape(key))

		_, err := doRequest("DELETE", requestURL)
		if err != nil {
			return err
		}

		fmt.Println("----Deleted---")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
