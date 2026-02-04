# SICLI - KV-Store CLI Client

A command-line interface for interacting with the KV-Store cluster.

## Installation

Build the CLI from the project root:

```bash
go build -o sicli ./cmd/cli
```

## Usage

### Basic Commands

#### Get a value
```bash
sicli get mykey
sicli get mykey --addr http://localhost:8081
```

#### Put a key-value pair
```bash
sicli put mykey myvalue
sicli put mykey myvalue --addr http://localhost:8081
sicli put "my key" "my value"
```

#### Delete a key
```bash
sicli delete mykey
sicli delete mykey --addr http://localhost:8081
```

### Configuration

#### Set configuration
```bash
sicli config set --server-url http://localhost:8081
sicli config set --timeout 60s
```

#### Show current configuration
```bash
sicli config show
```

Configuration is stored in `~/.sicli-config.json` and will be used as defaults for subsequent commands.

### Metrics

#### Display cluster metrics
```bash
sicli metrics
sicli metrics --format raw
sicli metrics --filter raft_current_term
```

### Testing

#### Run Vegeta load tests
```bash
sicli test vegeta --duration 30s --rate 100
sicli test vegeta --type put --duration 1m --rate 50
```

#### Run chaos tests
```bash
sicli test chaos --workers 5
```

## Global Flags

- `--addr`: Server address (default: http://localhost:8080)
- `--timeout`: Request timeout (default: 30s)
- `--help`: Show help information

## Examples

```bash
# Basic operations
sicli put user:123 john_doe
sicli get user:123
sicli delete user:123

# Using different server
sicli get user:123 --addr http://localhost:8081

# Configure for persistent settings
sicli config set --server-url http://production-cluster:8080
sicli config set --timeout 10s

# View metrics
sicli metrics --filter http_requests_total

# Load testing
sicli test vegeta --duration 2m --rate 200 --type put
```

## Error Handling

The CLI provides clear error messages for common issues:
- Connection failures
- Key not found (404)
- Server errors (5xx)
- Invalid parameters

Exit codes:
- 0: Success
- 1: Error occurred