package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

type Config struct {
	ServerURL string        `json:"server_url"`
	Timeout   time.Duration `json:"timeout"`
}

var (
	configFile   string
	setServerURL string
	setTimeout   time.Duration
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Configure CLI settings",
	Long:  `Manage CLI configuration settings like server URL and timeout.`,
}

var configSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set configuration values",
	Example: `  sicli config set --server-url http://localhost:8081
  sicli config set --timeout 60s`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadConfig()
		if err != nil {
			config = &Config{
				ServerURL: "http://localhost:8080",
				Timeout:   30 * time.Second,
			}
		}

		if setServerURL != "" {
			config.ServerURL = setServerURL
		}
		if setTimeout > 0 {
			config.Timeout = setTimeout
		}

		err = saveConfig(config)
		if err != nil {
			return fmt.Errorf("failed to save config: %v", err)
		}

		fmt.Println("Configuration updated")
		return nil
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadConfig()
		if err != nil {
			fmt.Println("No configuration found, using defaults:")
			config = &Config{
				ServerURL: "http://localhost:8080",
				Timeout:   30 * time.Second,
			}
		}

		fmt.Printf("Server URL: %s\n", config.ServerURL)
		fmt.Printf("Timeout: %s\n", config.Timeout)
		fmt.Printf("Config file: %s\n", getConfigPath())
		return nil
	},
}

func init() {
	configSetCmd.Flags().StringVar(&setServerURL, "server-url", "", "Set server URL")
	configSetCmd.Flags().DurationVar(&setTimeout, "timeout", 0, "Set request timeout")

	configCmd.AddCommand(configSetCmd)
	configCmd.AddCommand(configShowCmd)
	rootCmd.AddCommand(configCmd)
}

func getConfigPath() string {
	if configFile != "" {
		return configFile
	}
	
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ".sicli-config.json"
	}
	
	return filepath.Join(homeDir, ".sicli-config.json")
}

func loadConfig() (*Config, error) {
	configPath := getConfigPath()
	
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	
	return &config, nil
}

func saveConfig(config *Config) error {
	configPath := getConfigPath()
	
	// Create directory if it doesn't exist
	dir := filepath.Dir(configPath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(configPath, data, 0644)
}
