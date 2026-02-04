package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	baseURL string
	timeout time.Duration
)

var rootCmd = &cobra.Command{
	Use:   "sicli",
	Short: "KV-Store CLI client",
	Long:  `A command line interface for interacting with the KV-Store cluster.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Load config if it exists and flags weren't explicitly set
		if !cmd.Flags().Changed("addr") {
			if config, err := loadConfig(); err == nil {
				baseURL = config.ServerURL
			}
		}
		if !cmd.Flags().Changed("timeout") {
			if config, err := loadConfig(); err == nil {
				timeout = config.Timeout
			}
		}
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&baseURL, "addr", "http://localhost:8080", "Server address")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 30*time.Second, "Request timeout")
}

// doRequest performs HTTP request to the KV-Store server
func doRequest(method, url string) (string, error) {
	client := &http.Client{Timeout: timeout}
	
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}