package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestRotation(t *testing.T) {
	client := &http.Client{} // 1. Create a client

	// Send 500 requests
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-data-payload-%d-timestamp-%d", i, time.Now().UnixNano())

		url := fmt.Sprintf("http://localhost:8080/put?key=%s&val=%s", key, val)

		// 2. Create the Request with PUT method explicitly
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		// 3. Send the request
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		_ = resp.Body.Close()

		if i%50 == 0 {
			fmt.Printf("Sent %d requests...\n", i)
		}
	}
	fmt.Println("Done! Check your folder for multiple wal-*.log files.")
}
