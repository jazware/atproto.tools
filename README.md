# AT Proto Looking Glass

A collection of looking-glass tools for the AT Proto Network, featuring a high-performance firehose consumer backed by DuckDB.

## Services

### Looking Glass Consumer

The Looking Glass Consumer is a Go service that connects to an AT Proto Firehose and processes events in real-time.

**Key Features:**
- **DuckDB Backend**: Uses DuckDB for scalable analytics and efficient querying
- **Real-time Processing**: Connects to the AT Proto firehose and processes events as they arrive
- **Record Storage**: Unpacks and stores records from events with full JSON support
- **Event Tracking**: Maintains event metadata including timestamps, sequence numbers, and event types
- **Identity Resolution**: Tracks DID/handle/PDS mappings for accounts
- **TTL Management**: Automatically deletes old records to manage database size
- **HTTP API**: Exposes REST endpoints for querying records, events, and identities

#### Architecture

The consumer replaces the previous multi-backend approach (SQLite + Parquet + BigQuery) with a single, powerful DuckDB backend that provides:
- SQL-like query capabilities
- Excellent analytical performance
- Native JSON support
- Efficient compression
- ACID transactions

#### Running the Consumer

##### Using Just (recommended)

```bash
# Start the consumer
just lg-up

# Rebuild and start
just lg-rebuild

# Stop the consumer
just lg-down

# View logs
just lg-logs

# Run locally (without Docker)
just run-lg
```

##### Using Docker Compose directly

```bash
# Start the consumer
docker compose -f cmd/stream/docker-compose.yml up -d

# Stop the consumer
docker compose -f cmd/stream/docker-compose.yml down
```

##### Running from source

```bash
go run cmd/stream/main.go --help
```

#### Configuration

The consumer supports the following environment variables:

- `LG_WS_URL`: WebSocket URL for the firehose (default: `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos`)
- `LG_PORT`: HTTP server port (default: `8080`)
- `LG_DEBUG`: Enable debug logging (default: `false`)
- `LG_DUCKDB_PATH`: Path to DuckDB database file (default: `/data/looking-glass.db`)
- `LG_MIGRATE_DB`: Run database migrations on startup (default: `true`)
- `LG_EVT_RECORD_TTL`: Time-to-live for events and records (default: `72h`)
- `LG_PLC_RATE_LIMIT`: Rate limit for PLC lookups in requests/second (default: `100`)
- `LG_LOOKUP_ON_COMMIT`: Lookup DID docs on commit events (default: `false`)

#### API Endpoints

The consumer exposes the following HTTP endpoints:

- `GET /records` - Query records with filters:
  - `?did=<DID>` - Filter by repository DID
  - `?collection=<NSID>` - Filter by collection
  - `?rkey=<RecordKey>` - Filter by record key
  - `?seq=<number>` - Filter by firehose sequence
  - `?limit=<number>` - Limit results (max 1000)

- `GET /events` - Query events:
  - `?did=<DID>` - Filter by repository DID
  - `?event_type=<type>` - Filter by event type
  - `?seq=<number>` - Filter by firehose sequence
  - `?limit=<number>` - Limit results (max 1000)

- `GET /identities` - Query identities:
  - `?did=<DID>` - Filter by DID
  - `?handle=<handle>` - Filter by handle
  - `?pds=<PDS>` - Filter by PDS endpoint
  - `?limit=<number>` - Limit results (max 1000)

- `GET /metrics` - Prometheus metrics
- `GET /debug/pprof/*` - Go pprof profiling endpoints

#### Data Storage

The consumer stores data in a DuckDB database with the following tables:

- **records**: AT Protocol records with full JSON data
- **events**: Firehose event metadata
- **identities**: DID/handle/PDS mappings
- **cursors**: Firehose position tracking for resumption

By default, data is stored in `./data/looking-glass.db` when running locally, or in a Docker volume when using Docker Compose.

## Tools

### Checkout

The Checkout tool lets you download your AT Proto repository as a directory of JSON files (one per record).

It supports:
- Selecting a PDS to download from (defaults to the Relay at `bsky.network`)
- Compressing results into a gzipped tarball (recommended for large repos)

**Usage:**

```bash
go run cmd/checkout/main.go <repo-DID>

# With options
go run cmd/checkout/main.go --help
```

### PLC Exporter

Exports and monitors the PLC directory.

**Usage:**

```bash
# Start with Docker Compose
just plc-up

# Stop
just plc-down
```

## Development

### Prerequisites

- Go 1.23 or later
- Just (command runner) - optional but recommended
- Docker and Docker Compose - for containerized deployment

### Building

```bash
# Build all binaries
just build-all

# Build specific service
just build-lg

# Install dependencies
just deps
```

### Testing

```bash
# Run tests
just test

# Run tests with coverage
just test-coverage
```

### Code Quality

```bash
# Format code
just fmt

# Lint code
just lint

# Tidy dependencies
just tidy
```

## Migration from Previous Version

If you're upgrading from the previous SQLite/Parquet/BigQuery version:

1. **Data Migration**: The DuckDB schema is similar to the old SQLite schema. You can export data from SQLite and import into DuckDB if needed.

2. **Configuration Changes**:
   - `LG_SQLITE_PATH` → `LG_DUCKDB_PATH`
   - Removed: `LG_SQLITE_PERSIST`, `LG_PARQUET_DIR`, `LG_BIGQUERY_*` variables

3. **API Compatibility**: All HTTP endpoints remain the same, ensuring backward compatibility.

## License

See [LICENSE](LICENSE) for details.
