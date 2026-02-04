package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	metricsFormat string
	metricsFilter string
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Display cluster metrics",
	Long:  `Fetch and display metrics from the KV-Store cluster. Metrics are scraped from the Prometheus endpoint.`,
	Example: `  sicli metrics
  sicli metrics --format raw
  sicli metrics --filter raft_current_term`,
	RunE: func(cmd *cobra.Command, args []string) error {
		requestURL := fmt.Sprintf("%s/metrics", baseURL)

		metrics, err := doRequest("GET", requestURL)
		if err != nil {
			return err
		}

		if metricsFormat == "raw" {
			if metricsFilter != "" {
				lines := strings.Split(metrics, "\n")
				var filtered []string
				for _, line := range lines {
					if strings.Contains(line, metricsFilter) {
						filtered = append(filtered, line)
					}
				}
				fmt.Println(strings.Join(filtered, "\n"))
			} else {
				fmt.Println(metrics)
			}
			return nil
		}

		// Parse and format metrics for better readability
		err = displayFormattedMetrics(metrics, metricsFilter)
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	metricsCmd.Flags().StringVar(&metricsFormat, "format", "formatted", "Output format: formatted or raw")
	metricsCmd.Flags().StringVar(&metricsFilter, "filter", "", "Filter metrics by name pattern")
	rootCmd.AddCommand(metricsCmd)
}

func displayFormattedMetrics(metrics, filter string) error {
	lines := strings.Split(metrics, "\n")
	
	fmt.Println("=== KV-Store Cluster Metrics ===\n")
	
	categories := map[string][]string{
		"Raft Metrics": {},
		"KV Store Metrics": {},
		"HTTP Metrics": {},
		"System Metrics": {},
	}
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		
		if filter != "" && !strings.Contains(line, filter) {
			continue
		}
		
		switch {
		case strings.Contains(line, "raft_"):
			categories["Raft Metrics"] = append(categories["Raft Metrics"], line)
		case strings.Contains(line, "kv_"):
			categories["KV Store Metrics"] = append(categories["KV Store Metrics"], line)
		case strings.Contains(line, "http_"):
			categories["HTTP Metrics"] = append(categories["HTTP Metrics"], line)
		default:
			categories["System Metrics"] = append(categories["System Metrics"], line)
		}
	}
	
	for category, metricLines := range categories {
		if len(metricLines) > 0 {
			fmt.Printf("%s:\n", category)
			for _, metric := range metricLines {
				parts := strings.Fields(metric)
				if len(parts) >= 2 {
					name := parts[0]
					value := parts[1]
					fmt.Printf("  %-30s %s\n", name, value)
				}
			}
			fmt.Println()
		}
	}
	
	return nil
}
