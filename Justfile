# Justfile for AT Proto Tools

# Get the current git commit hash
version := `git rev-parse HEAD | cut -c1-7`

# Default recipe to display available commands
default:
    @just --list

# Build the Looking Glass Consumer
build-lg:
    @echo "Building Looking Glass Consumer..."
    @cd cmd/stream && go build -o ../../bin/lg-consumer

# Run the Looking Glass Consumer locally
run-lg:
    @echo "Running Looking Glass Consumer..."
    @go run cmd/stream/main.go

# Start up the Looking Glass Consumer with Docker Compose
lg-up:
    @echo "Starting up the Looking Glass Consumer (sha-{{version}})"
    @VERSION=sha-{{version}} docker compose -f cmd/stream/docker-compose.yml up -d

# Rebuild and start the Looking Glass Consumer
lg-rebuild:
    @echo "Rebuilding and starting Looking Glass Consumer (sha-{{version}})"
    @VERSION=sha-{{version}} docker compose -f cmd/stream/docker-compose.yml up -d --build

# Stop the Looking Glass Consumer
lg-down:
    @echo "Shutting down the Looking Glass Consumer"
    @VERSION=sha-{{version}} docker compose -f cmd/stream/docker-compose.yml down

# View Looking Glass Consumer logs
lg-logs:
    @docker compose -f cmd/stream/docker-compose.yml logs -f

# Start up the PLC Exporter
plc-up:
    @echo "Starting up the PLC Exporter (sha-{{version}})"
    @VERSION=sha-{{version}} docker compose -f cmd/plc/docker-compose.yml up -d --build

# Stop the PLC Exporter
plc-down:
    @echo "Shutting down the PLC Exporter"
    @VERSION=sha-{{version}} docker compose -f cmd/plc/docker-compose.yml down

# Run tests
test:
    @echo "Running tests..."
    @go test ./...

# Run tests with coverage
test-coverage:
    @echo "Running tests with coverage..."
    @go test -cover ./...

# Format code
fmt:
    @echo "Formatting code..."
    @go fmt ./...

# Lint code
lint:
    @echo "Linting code..."
    @golangci-lint run

# Tidy dependencies
tidy:
    @echo "Tidying dependencies..."
    @go mod tidy

# Clean build artifacts
clean:
    @echo "Cleaning build artifacts..."
    @rm -rf bin/
    @rm -rf data/

# Build all binaries
build-all: build-lg
    @echo "Building all binaries..."
    @cd cmd/checkout && go build -o ../../bin/checkout
    @cd cmd/plc && go build -o ../../bin/plc-exporter

# Install dependencies
deps:
    @echo "Installing dependencies..."
    @go mod download
