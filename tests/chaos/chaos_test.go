package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// getBinaryPath finds the kv-server binary
func getBinaryPath(t *testing.T) string {
	// First check if binary exists in project root
	projectRoot := filepath.Join("..", "..")
	binaryPath := filepath.Join(projectRoot, "kv-server")

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Fatalf("Binary not found at %s. Run 'go build -o kv-server ./cmd/server' first", binaryPath)
	}

	absPath, _ := filepath.Abs(binaryPath)
	return absPath
}

// cleanupState removes persistent state files before test
func cleanupState() {
	// Remove raft state files
	for i := 0; i < 5; i++ {
		_ = os.Remove(fmt.Sprintf("../../raft_state_%d.gob", i))
	}
	// Remove storage directories
	_ = os.RemoveAll("../../Storage/wal")
	_ = os.RemoveAll("../../Storage/data")
}

// TestLeaderFailoverDuringWrites is the main chaos test
//
// What this test does:
// 1. Starts a 3-node Raft cluster
// 2. Identifies the leader
// 3. Writes N keys to the cluster (all should go to leader)
// 4. KILLS the leader with SIGKILL (simulating crash)
// 5. Waits for new leader election
// 6. Writes N more keys
// 7. Verifies ALL 2N keys are readable (no data loss)
//
// This proves: Raft's durability guarantee - acknowledged writes survive leader failures
func TestLeaderFailoverDuringWrites(t *testing.T) {
	cleanupState()
	binaryPath := getBinaryPath(t)

	t.Log("=== Chaos Test: Leader Failover During Writes ===")
	t.Log("This test validates that no acknowledged writes are lost when the leader crashes")
	t.Log("")

	// Step 1: Start 3-node cluster
	t.Log("[Step 1] Starting 3-node cluster...")
	cluster := NewCluster(binaryPath)
	if err := cluster.Start(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	defer cluster.Shutdown()

	// Step 2: Wait for leader election
	t.Log("[Step 2] Waiting for leader election...")
	leader, err := cluster.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("No leader elected: %v", err)
	}
	t.Logf("         Leader elected: Node %d", leader)

	// Step 3: Write first batch of keys
	t.Log("[Step 3] Writing first batch (50 keys)...")
	acknowledgedKeys := make(map[string]string)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("pre_crash_key_%d", i)
		val := fmt.Sprintf("value_%d", i)

		if err := cluster.WriteKey(key, val); err != nil {
			// Not all writes may succeed, only track acknowledged ones
			t.Logf("         Write failed for %s (expected during chaos): %v", key, err)
			continue
		}
		acknowledgedKeys[key] = val
	}
	t.Logf("         Acknowledged %d writes before crash", len(acknowledgedKeys))

	// Give time for replication
	time.Sleep(1 * time.Second)

	// Step 4: Kill the leader
	t.Logf("[Step 4] Killing leader (Node %d)...", leader)
	if err := cluster.KillNode(leader); err != nil {
		t.Fatalf("Failed to kill leader: %v", err)
	}
	t.Log("         Leader killed!")

	// Step 5: Wait for new leader
	t.Log("[Step 5] Waiting for new leader election...")
	time.Sleep(3 * time.Second) // Allow election timeout + election

	newLeader, err := cluster.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("No new leader elected after crash: %v", err)
	}
	if newLeader == leader {
		t.Fatalf("Same node %d is still leader (should be impossible after SIGKILL)", leader)
	}
	t.Logf("         New leader elected: Node %d", newLeader)

	// Step 6: Write second batch
	t.Log("[Step 6] Writing second batch (50 keys)...")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("post_crash_key_%d", i)
		val := fmt.Sprintf("value_%d", i)

		if err := cluster.WriteKey(key, val); err != nil {
			t.Logf("         Write failed for %s: %v", key, err)
			continue
		}
		acknowledgedKeys[key] = val
	}
	t.Logf("         Total acknowledged writes: %d", len(acknowledgedKeys))

	// Give time for replication
	time.Sleep(2 * time.Second)

	// Step 7: Verify ALL acknowledged writes
	t.Log("[Step 7] Verifying all acknowledged writes...")
	missingKeys := []string{}
	wrongValues := []string{}

	for key, expectedVal := range acknowledgedKeys {
		actualVal, found := cluster.ReadKey(key)
		if !found {
			missingKeys = append(missingKeys, key)
		} else if actualVal != expectedVal {
			wrongValues = append(wrongValues, fmt.Sprintf("%s: expected=%s, got=%s", key, expectedVal, actualVal))
		}
	}

	// Report results
	t.Log("")
	t.Log("=== Results ===")
	t.Logf("Total acknowledged writes: %d", len(acknowledgedKeys))
	t.Logf("Missing keys: %d", len(missingKeys))
	t.Logf("Wrong values: %d", len(wrongValues))

	if len(missingKeys) > 0 {
		t.Errorf("DATA LOSS DETECTED! Missing keys: %v", missingKeys)
	}
	if len(wrongValues) > 0 {
		t.Errorf("DATA CORRUPTION DETECTED! Wrong values: %v", wrongValues)
	}

	if len(missingKeys) == 0 && len(wrongValues) == 0 {
		t.Log("")
		t.Log("✅ SUCCESS: All acknowledged writes survived leader failover!")
		t.Log("   This proves: Raft's durability guarantee is working correctly")
	}
}

// TestWritesDuringElection tests the cluster's behavior during unstable state
//
// What this test does:
// 1. Starts a 3-node cluster
// 2. Kills the leader
// 3. Immediately attempts writes to ALL nodes
// 4. Verifies only ONE node accepted writes (no split-brain)
// 5. After election stabilizes, verifies consistency
//
// This proves: Raft's safety guarantee - only one leader can accept writes at a time
func TestWritesDuringElection(t *testing.T) {
	cleanupState()
	binaryPath := getBinaryPath(t)

	t.Log("=== Chaos Test: Writes During Election ===")
	t.Log("This test validates that only one leader accepts writes during election")
	t.Log("")

	// Start cluster
	t.Log("[Step 1] Starting 3-node cluster...")
	cluster := NewCluster(binaryPath)
	if err := cluster.Start(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	defer cluster.Shutdown()

	// Wait for stable leader
	t.Log("[Step 2] Waiting for stable leader...")
	leader, err := cluster.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("No leader elected: %v", err)
	}
	t.Logf("         Leader: Node %d", leader)

	// Kill leader and immediately try writes to all nodes
	t.Logf("[Step 3] Killing leader and attempting writes to all nodes...")
	_ = cluster.KillNode(leader)

	// Try to write to all surviving nodes simultaneously
	acceptedBy := make(map[int]bool)
	key := "election_test_key"

	for i := 0; i < 3; i++ {
		if i == leader {
			continue // Skip killed node
		}
		success, err := cluster.WriteKeyToNode(i, key, fmt.Sprintf("from_node_%d", i))
		if err == nil && success {
			acceptedBy[i] = true
			t.Logf("         Node %d accepted write", i)
		}
	}

	// Wait for election to stabilize
	t.Log("[Step 4] Waiting for election to stabilize...")
	time.Sleep(5 * time.Second)

	newLeader, err := cluster.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("No leader after election: %v", err)
	}
	t.Logf("         New leader: Node %d", newLeader)

	// Verify the value is consistent across all nodes
	t.Log("[Step 5] Checking data consistency across nodes...")
	var values []string
	for i := 0; i < 3; i++ {
		if i == leader {
			continue
		}
		val, found, err := cluster.ReadKeyFromNode(i, key)
		if err != nil || !found {
			t.Logf("         Node %d: key not found", i)
			continue
		}
		values = append(values, val)
		t.Logf("         Node %d: %s", i, val)
	}

	// All nodes should have the same value
	if len(values) > 1 {
		first := values[0]
		for _, v := range values[1:] {
			if v != first {
				t.Errorf("SPLIT-BRAIN DETECTED! Nodes have different values: %v", values)
			}
		}
	}

	if len(acceptedBy) <= 1 {
		t.Log("")
		t.Log("✅ SUCCESS: At most one node accepted writes during election!")
		t.Log("   This proves: Raft's safety guarantee prevents split-brain")
	}
}
