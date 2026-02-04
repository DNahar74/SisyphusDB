package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	testDuration string
	testRate     string
	testType     string
	testWorkers  int
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run performance and chaos tests",
	Long:  `Run various tests against the KV-Store cluster including load tests and chaos tests.`,
}

var vegetaCmd = &cobra.Command{
	Use:   "vegeta",
	Short: "Run Vegeta load tests",
	Long:  `Run Vegeta load tests against the KV-Store cluster.`,
	Example: `  sicli test vegeta --duration 30s --rate 100
  sicli test vegeta --type put --duration 1m --rate 50`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check if vegeta is installed
		_, err := exec.LookPath("vegeta")
		if err != nil {
			return fmt.Errorf("vegeta is not installed. Please install it first: go install github.com/tsenart/vegeta/v12@latest")
		}

		var targetURL string
		var httpMethod string
		switch testType {
		case "get":
			targetURL = fmt.Sprintf("%s/get?key=testkey", baseURL)
			httpMethod = "GET"
		case "put":
			targetURL = fmt.Sprintf("%s/put?key=testkey&val=testvalue", baseURL)
			httpMethod = "PUT"
		case "delete":
			targetURL = fmt.Sprintf("%s/delete?key=testkey", baseURL)
			httpMethod = "DELETE"

		default:
			targetURL = fmt.Sprintf("%s/get?key=testkey", baseURL)
			httpMethod = "GET"
		}

		fmt.Printf("Running Vegeta load test...\n")
		fmt.Printf("Target: %s %s\n", httpMethod, targetURL)
		fmt.Printf("Duration: %s\n", testDuration)
		fmt.Printf("Rate: %s requests/second\n", testRate)
		fmt.Println()

		// Create vegeta attack command
		attackCmd := exec.Command("vegeta", "attack", 
			"-duration", testDuration,
			"-rate", testRate,
			"-targets", "-")
		
		attackCmd.Stdin = strings.NewReader(fmt.Sprintf("%s %s\n", httpMethod, targetURL))
		
		// Pipe to vegeta report
		reportCmd := exec.Command("vegeta", "report")
		
		// Connect the commands
		pipe, err := attackCmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("failed to create pipe: %v", err)
		}
		
		reportCmd.Stdin = pipe
		reportCmd.Stdout = os.Stdout
		reportCmd.Stderr = os.Stderr
		
		// Start both commands
		err = attackCmd.Start()
		if err != nil {
			return fmt.Errorf("failed to start attack: %v", err)
		}
		
		err = reportCmd.Start()
		if err != nil {
			return fmt.Errorf("failed to start report: %v", err)
		}
		
		// Wait for completion
		err = attackCmd.Wait()
		if err != nil {
			return fmt.Errorf("attack failed: %v", err)
		}
		
		err = reportCmd.Wait()
		if err != nil {
			return fmt.Errorf("report failed: %v", err)
		}

		return nil
	},
}

var chaosCmd = &cobra.Command{
	Use:   "chaos",
	Short: "Run chaos tests",
	Long:  `Run chaos tests to verify cluster resilience.`,
	Example: `  sicli test chaos --workers 5`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Running chaos tests with %d workers...\n", testWorkers)
		fmt.Printf("Target cluster: %s\n", baseURL)
		fmt.Println()

		// Check if the chaos test binary exists
		chaosTestPath := "tests/chaos/chaos.test"
		if _, err := os.Stat(chaosTestPath); os.IsNotExist(err) {
			fmt.Println("Building chaos test binary...")
			buildCmd := exec.Command("go", "test", "-c", "./tests/chaos")
			buildCmd.Dir = "."
			err := buildCmd.Run()
			if err != nil {
				return fmt.Errorf("failed to build chaos test: %v", err)
			}
		}

		// Run the chaos test
		chaosCmd := exec.Command("./tests/chaos/chaos.test", 
			"-test.v",
			"-workers", strconv.Itoa(testWorkers),
			"-addr", baseURL)
		
		chaosCmd.Stdout = os.Stdout
		chaosCmd.Stderr = os.Stderr
		
		err := chaosCmd.Run()
		if err != nil {
			return fmt.Errorf("chaos test failed: %v", err)
		}

		return nil
	},
}

func init() {
	vegetaCmd.Flags().StringVar(&testDuration, "duration", "30s", "Test duration")
	vegetaCmd.Flags().StringVar(&testRate, "rate", "50", "Request rate per second")
	vegetaCmd.Flags().StringVar(&testType, "type", "get", "Test type: get, put, delete")

	chaosCmd.Flags().IntVar(&testWorkers, "workers", 3, "Number of chaos test workers")

	testCmd.AddCommand(vegetaCmd)
	testCmd.AddCommand(chaosCmd)
	rootCmd.AddCommand(testCmd)
}