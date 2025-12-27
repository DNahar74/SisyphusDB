# --- Stage 1: Builder ---
# Use the official Go image to compile
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy dependency files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the binary.
# CGO_ENABLED=0 creates a static binary (no external C library dependencies)
RUN CGO_ENABLED=0 GOOS=linux go build -o kv-server cmd/server/main.go

# --- Stage 2: Runner ---
# Use a tiny Alpine Linux image
FROM alpine:latest

WORKDIR /root/

# Copy only the compiled binary from Stage 1
COPY --from=builder /app/kv-server .

# Create directories for data persistence
RUN mkdir -p Storage/wal Storage/data

# Expose ports (Documentation only, technically)
EXPOSE 8000-8003 5000-5003

# Default command (can be overridden)
ENTRYPOINT ["./kv-server"]