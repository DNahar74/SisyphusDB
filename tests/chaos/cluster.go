package chaos

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Node represents a single KV-Store node process
type Node struct {
	ID       int
	Process  *exec.Cmd
	HTTPPort string
	RPCPort  string
}

// Cluster manages multiple KV-Store nodes for testing
type Cluster struct {
	Nodes      []*Node
	mu         sync.Mutex
	BinaryPath string
}

// NewCluster creates a new test cluster configuration
func NewCluster(binaryPath string) *Cluster {
	return &Cluster{
		Nodes:      make([]*Node, 0),
		BinaryPath: binaryPath,
	}
}

// Start spawns n nodes as separate processes
func (c *Cluster) Start(n int) error {
	// Build peer addresses for gRPC
	var rpcAddrs []string
	for i := 0; i < n; i++ {
		rpcAddrs = append(rpcAddrs, fmt.Sprintf("localhost:%d", 5001+i))
	}
	peerStr := strings.Join(rpcAddrs, ",")

	for i := 0; i < n; i++ {
		httpPort := fmt.Sprintf("%d", 8001+i)
		rpcPort := fmt.Sprintf("%d", 5001+i)

		cmd := exec.Command(c.BinaryPath,
			"-id", fmt.Sprintf("%d", i),
			"-peers", peerStr,
			"-port", rpcPort,
			"-http", httpPort,
			"-peer-template", "http://localhost:%d",
		)

		// Redirect output for debugging
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start node %d: %w", i, err)
		}

		node := &Node{
			ID:       i,
			Process:  cmd,
			HTTPPort: httpPort,
			RPCPort:  rpcPort,
		}
		c.Nodes = append(c.Nodes, node)
	}

	// Wait for nodes to start up
	time.Sleep(2 * time.Second)
	return nil
}

// KillNode sends SIGKILL to a specific node
func (c *Cluster) KillNode(id int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if id >= len(c.Nodes) || c.Nodes[id] == nil {
		return fmt.Errorf("node %d not found", id)
	}

	node := c.Nodes[id]
	if node.Process == nil || node.Process.Process == nil {
		return fmt.Errorf("node %d not running", id)
	}

	// SIGKILL for immediate termination (simulates crash)
	if err := node.Process.Process.Signal(syscall.SIGKILL); err != nil {
		return fmt.Errorf("failed to kill node %d: %w", id, err)
	}

	// Wait for process to exit
	_ = node.Process.Wait()
	c.Nodes[id].Process = nil
	return nil
}

// Shutdown gracefully stops all nodes
func (c *Cluster) Shutdown() {
	for _, node := range c.Nodes {
		if node != nil && node.Process != nil && node.Process.Process != nil {
			_ = node.Process.Process.Signal(syscall.SIGTERM)
			_ = node.Process.Wait()
		}
	}
}

// GetLeader queries nodes to find the current leader
// Returns leader ID or -1 if no leader found
func (c *Cluster) GetLeader() int {
	for _, node := range c.Nodes {
		if node == nil || node.Process == nil {
			continue
		}

		// Try a simple health check / write to determine leader
		url := fmt.Sprintf("http://localhost:%s/put?key=__leader_check__&val=1", node.HTTPPort)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			return node.ID
		}
	}
	return -1
}

// WriteKey writes a key-value pair to the cluster
// Returns nil on success, error otherwise
func (c *Cluster) WriteKey(key, val string) error {
	// Find a live node and try to write
	for _, node := range c.Nodes {
		if node == nil || node.Process == nil {
			continue
		}

		url := fmt.Sprintf("http://localhost:%s/put?key=%s&val=%s", node.HTTPPort, key, val)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			return nil
		}
		// If not leader, the request might be forwarded internally
		// Give it a moment and try again
	}
	return fmt.Errorf("no node accepted write for key %s", key)
}

// WriteKeyToNode writes to a specific node
func (c *Cluster) WriteKeyToNode(nodeID int, key, val string) (bool, error) {
	if nodeID >= len(c.Nodes) || c.Nodes[nodeID] == nil || c.Nodes[nodeID].Process == nil {
		return false, fmt.Errorf("node %d not available", nodeID)
	}

	url := fmt.Sprintf("http://localhost:%s/put?key=%s&val=%s", c.Nodes[nodeID].HTTPPort, key, val)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200, nil
}

// ReadKey reads a key from any available node
func (c *Cluster) ReadKey(key string) (string, bool) {
	for _, node := range c.Nodes {
		if node == nil || node.Process == nil {
			continue
		}

		url := fmt.Sprintf("http://localhost:%s/get?key=%s", node.HTTPPort, key)
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			body, _ := io.ReadAll(resp.Body)
			return string(body), true
		}
		if resp.StatusCode == 404 {
			return "", false
		}
	}
	return "", false
}

// ReadKeyFromNode reads from a specific node
func (c *Cluster) ReadKeyFromNode(nodeID int, key string) (string, bool, error) {
	if nodeID >= len(c.Nodes) || c.Nodes[nodeID] == nil || c.Nodes[nodeID].Process == nil {
		return "", false, fmt.Errorf("node %d not available", nodeID)
	}

	url := fmt.Sprintf("http://localhost:%s/get?key=%s", c.Nodes[nodeID].HTTPPort, key)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		return string(body), true, nil
	}
	return "", false, nil
}

// WaitForLeader polls until a leader is elected or timeout
func (c *Cluster) WaitForLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := c.GetLeader()
		if leader >= 0 {
			return leader, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return -1, fmt.Errorf("no leader elected within %v", timeout)
}

// LiveNodes returns count of nodes still running
func (c *Cluster) LiveNodes() int {
	count := 0
	for _, node := range c.Nodes {
		if node != nil && node.Process != nil {
			count++
		}
	}
	return count
}
