# Chaos Tests for KV-Store

This directory contains chaos tests that validate the correctness of the Raft consensus implementation under failure scenarios.

## What These Tests Prove

| Test | What It Does | What It Proves |
|------|--------------|----------------|
| `TestLeaderFailoverDuringWrites` | Kills leader mid-write, verifies no data loss | Raft's **durability guarantee** |
| `TestWritesDuringElection` | Sends writes during unstable state | Raft's **safety guarantee** (no split-brain) |

## Running the Tests

### Prerequisites

Build the server binary first:
```bash
cd ../..
go build -o kv-server ./cmd/server
```

### Run Tests

```bash
cd tests/chaos
go test -v -timeout 120s
```

### Expected Output

```
=== RUN   TestLeaderFailoverDuringWrites
=== Chaos Test: Leader Failover During Writes ===
[Step 1] Starting 3-node cluster...
[Step 2] Waiting for leader election...
         Leader elected: Node 0
[Step 3] Writing first batch (50 keys)...
         Acknowledged 50 writes before crash
[Step 4] Killing leader (Node 0)...
         Leader killed!
[Step 5] Waiting for new leader election...
         New leader elected: Node 1
[Step 6] Writing second batch (50 keys)...
         Total acknowledged writes: 100
[Step 7] Verifying all acknowledged writes...

=== Results ===
Total acknowledged writes: 100
Missing keys: 0
Wrong values: 0

✅ SUCCESS: All acknowledged writes survived leader failover!
--- PASS: TestLeaderFailoverDuringWrites (15.34s)
```

## Understanding the Tests

### Leader Failover Test

This test answers the question: **"If the leader dies, do we lose data?"**

```
Timeline:
[0s]     Start 3-node cluster
[2s]     Wait for leader election
[3s]     Write 50 keys (all acknowledged)
[4s]     SIGKILL the leader
[7s]     Wait for new leader
[8s]     Write 50 more keys
[10s]    Read back ALL 100 keys
         → PASS if all present, FAIL if any missing
```

### Election Safety Test

This test answers: **"Can two nodes both think they're leader?"**

```
Timeline:
[0s]     Start cluster, identify leader
[2s]     Kill leader
[2.1s]   Try to write to ALL remaining nodes
         → At most ONE should accept
[5s]     Verify all nodes have same data
```

## Troubleshooting

- **"Binary not found"**: Run `go build -o kv-server ./cmd/server` from project root
- **Port conflicts**: The tests use ports 5001-5003 (gRPC) and 8001-8003 (HTTP)
- **Flaky tests**: Increase timeouts in `WaitForLeader()` if on slow hardware
