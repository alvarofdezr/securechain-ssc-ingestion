# Secure Chain SSC Ingestion

[![License](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Lint & Test](https://github.com/securechaindev/securechain-ssc-ingestion/actions/workflows/lint-test.yml/badge.svg)]()
[![GHCR](https://img.shields.io/badge/GHCR-securechain--ssc--ingestion-blue?logo=docker)](https://github.com/orgs/securechaindev/packages/container/package/securechain-ssc-ingestion)

Data pipeline for ingesting and updating software packages from multiple ecosystems into SecureChain.

## Overview

This project extracts, processes, and ingests package data from seven major software registries: PyPI (Python), NPM (Node.js), Maven (Java), Cargo (Rust), RubyGems (Ruby), NuGet (.NET), and Go (Go modules). The data is stored in Neo4j for dependency graph analysis and MongoDB for vulnerability information.

Built with **Dagster 1.12.6** for modern data orchestration, providing a clean asset-centric approach with automatic data lineage tracking, scheduling capabilities, and comprehensive monitoring.

## Key Features

- 🔄 **Dual Operation Modes**:
  - **Ingestion**: Bulk import from registries for most ecosystems; cursor-based incremental for Go (~5.23M+ total packages)
  - **Updates**: Daily incremental updates for existing packages

- 📊 **7 Package Ecosystems**: PyPI, NPM, Maven, NuGet, Cargo, RubyGems, Go
- 🗄️ **Graph Storage**: Neo4j for package relationships and dependency graphs
- 🔐 **Vulnerability Tracking**: MongoDB for security advisories
- 🔍 **Import Names Extraction**: Automatic extraction of importable modules/classes for all packages
- ⚡ **Performance Optimized**: Set-based deduplication, caching (1hr TTL for listings, 7-day for import_names), batch processing
- 📅 **Smart Scheduling**: Staggered execution times to avoid resource conflicts
- 📈 **Rich Metrics**: Ingestion rates, error tracking, success rates per ecosystem

## Tech Stack

- **Dagster 1.12.6** - Modern data orchestrator with web UI
- **Python 3.13** - Runtime environment with JIT compiler and improved performance
- **UV** - Ultra-fast Python package manager (10-100x faster than pip)
- **Neo4j** - Graph database for package relationships
- **MongoDB** - Document database for vulnerability data
- **Redis 7** - Message queue, stream processing, and ingestion cursor storage
- **PostgreSQL 17** - Dagster metadata storage
- **Docker** - Containerization platform

## Docker Services

This project runs **4 containerized services**:

1. **dagster-postgres** (postgres:17)
   - Stores Dagster metadata (runs, events, schedules)
   - Port: 5432 (internal)
   - Volume: `dagster_postgres_data`

2. **redis** (redis:7-alpine)
   - Message queue for asynchronous package extraction
   - Cursor storage for resumable Go ingestion
   - Port: 6379 (exposed)
   - Volume: `redis_data`
   - Persistence: AOF (Append Only File) enabled

3. **dagster-daemon**
   - Processes schedules and sensors
   - Depends on: postgres, redis
   - No exposed ports

4. **dagster-webserver**
   - Web UI for monitoring and management
   - Port: 3000 (exposed)
   - Depends on: postgres, redis, daemon

**External Network**: `securechain` (must exist, connects to Neo4j/MongoDB)

## Before Start

First, create a Docker network for containers:

```bash
docker network create securechain
```

Then, download the zipped [data dumps](https://doi.org/10.5281/zenodo.17131401) from Zenodo for graphs and vulnerabilities information. Once you have unzipped the dumps, run:

```bash
docker compose up -d
```

The containerized databases will be seeded automatically.

## Quick Start

### Production (Docker) - 3 steps

```bash
# 1. Configure environment
cp .env.template .env
nano .env  # Update passwords and connection strings

# 2. Start all services (Dagster + Redis)
docker compose up -d

# 3. Access Dagster UI
# Open http://localhost:3000 in your browser
```

### Development (Local) - Using UV

```bash
# 1. Install UV (one-time setup)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Install dependencies
uv sync

# 3. Run Dagster locally
uv run dagster dev -m src.dagster_app
```

Open http://localhost:3000 to access the Dagster UI.

**Services started**:
- ✅ Neo4j (Graph database)
- ✅ MongoDB (Vulnerability database)
- ✅ PostgreSQL (Dagster metadata)
- ✅ Redis (Message queue + cursor store)
- ✅ Dagster Daemon (Scheduler)
- ✅ Dagster Webserver (UI)

## Available Assets

### Ingestion Assets (Weekly - STOPPED by default)

One-time bulk ingestion of all packages from registries. Run these manually when you need to populate the graph with new ecosystems or rebuild from scratch.

| Asset | Ecosystem | Schedule | Volume | Description |
|-------|-----------|----------|--------|-------------|
| `pypi_package_ingestion` | Python | Sun 2:00 AM | ~500k | Ingests all PyPI packages |
| `npm_package_ingestion` | Node.js | Sun 3:00 AM | ~3M | Ingests all NPM packages |
| `maven_package_ingestion` | Java | Sun 4:00 AM | ~500k-1M | Ingests unique Maven artifacts |
| `nuget_package_ingestion` | .NET | Sun 5:00 AM | ~400k | Ingests all NuGet packages |
| `cargo_package_ingestion` | Rust | Sun 6:00 AM | ~150k | Ingests all Cargo crates |
| `rubygems_package_ingestion` | Ruby | Sun 7:00 AM | ~180k | Ingests all RubyGems |
| `go_package_ingestion` | Go | Sun 8:00 AM | ~500k+ | Cursor-based incremental ingestion from index.golang.org |

**Total Packages**: ~5.23 million packages across all ecosystems (Go volume grows continuously)

> **Go ingestion is resumable**: the asset stores a timestamp cursor in Redis under the key `go_ingestion_cursor`. Each run continues from where the previous one stopped, defaulting to the Go proxy launch date (`2019-04-10`) on first run. The cursor is updated after every successful batch.

### Update Assets (Daily - RUNNING by default)

Daily incremental updates for existing packages in the graph. These run automatically to keep package versions current.

| Asset | Ecosystem | Schedule | Description |
|-------|-----------|----------|-------------|
| `pypi_packages_updates` | Python | Daily 10:00 AM | Updates Python packages from PyPI |
| `npm_packages_updates` | Node.js | Daily 12:00 PM | Updates JavaScript packages from NPM |
| `maven_packages_updates` | Java | Daily 2:00 PM | Updates Java packages from Maven Central |
| `cargo_packages_updates` | Rust | Daily 4:00 PM | Updates Rust crates from crates.io |
| `rubygems_packages_updates` | Ruby | Daily 6:00 PM | Updates Ruby gems from RubyGems |
| `nuget_packages_updates` | .NET | Daily 8:00 PM | Updates .NET packages from NuGet |
| `go_packages_updates` | Go | Daily 9:00 PM | Updates Go module versions from proxy.golang.org |

### Redis Queue Processor (Every 5 minutes - RUNNING by default)

Asynchronous package processing by consuming extraction messages from Redis queue.

| Asset | Purpose | Schedule | Description |
|-------|---------|----------|-------------|
| `redis_queue_processor` | Queue Processing | Every 5 min | Reads package extraction messages from Redis and routes to appropriate extractors |

**How it works**:
1. Reads messages from Redis stream (`package-extraction`) in batches of 100
2. Validates each message using `PackageMessageSchema`
3. Routes to the correct extractor based on `node_type` (PyPIPackage, NPMPackage, GoPackage, etc.)
4. Acknowledges successful processing or moves failed messages to dead-letter queue
5. Reports metrics: processed, successful, failed, validation errors, unsupported types

**Use cases**:
- **Dependency Discovery**: Queue dependencies during package analysis
- **On-Demand Ingestion**: External systems request package extraction via Redis
- **Retry Mechanism**: Re-queue failed extractions
- **Load Distribution**: Multiple consumers process in parallel

All schedules can be enabled/disabled individually from the Dagster UI (`Automation` tab).

## Architecture

### Data Flow

```
┌─────────────────┐
│ Package Registry│  (PyPI, NPM, Maven, NuGet, Cargo, RubyGems, Go)
└────────┬────────┘
         │ HTTP API
         ↓
┌─────────────────┐
│  Dagster Asset  │  (Ingestion / Update)
└────────┬────────┘
         │
         ├─→ Ingestion: Fetch all → Check existence → Extract if new
         │   Go: cursor-based  → fetch batch since timestamp → process → advance cursor
         │
         ├─→ Update: Batch read existing → Fetch versions → Update nodes
         │
         ├─→ Import Names: Download package → Extract modules/classes → Cache (7 days)
         │
         └─→ Queue: Write extraction messages to Redis
         │
         ↓
┌─────────────────────┐
│ Redis Stream        │  (package-extraction)
│ - Messages queued   │
│ - Consumer group    │
│ - Go cursor key     │
└────────┬────────────┘
         │ Every 5 minutes
         ↓
┌─────────────────────┐
│ redis_queue_        │
│ processor           │
│ - Read batch (100)  │
│ - Validate schema   │
│ - Route to extractor│
└────────┬────────────┘
         │
         ├─→ PyPIPackageExtractor
         ├─→ NPMPackageExtractor
         ├─→ MavenPackageExtractor
         ├─→ NuGetPackageExtractor
         ├─→ CargoPackageExtractor
         ├─→ RubyGemsPackageExtractor
         └─→ GoPackageExtractor
         │
         ↓
┌─────────────────┐
│ Neo4j + MongoDB │  (Graph storage + Vulnerabilities)
└─────────────────┘
```

### Message Flow (Redis Queue)

```json
{
  "node_type": "PyPIPackage",
  "package": "requests",
  "vendor": "Kenneth Reitz",
  "repository_url": "https://github.com/psf/requests",
  "constraints": ">=2.0.0",
  "parent_id": "abc123",
  "refresh": false
}
```

**Error Handling**:
- Validation errors → Dead-letter queue (`package-extraction-dlq`)
- Unsupported types → Dead-letter queue with error details
- Processing failures → Dead-letter queue for manual review

### Registry-Specific Implementation

Each ecosystem has unique characteristics handled by specialized API clients with optimized extraction methods:

#### PyPI (Python)
- **Method**: HTML parsing of Simple API (`/simple/` endpoint)
- **Extraction**: Regex pattern matching for package links
- **Optimization**: Single page lists all packages (~500k)
- **Volume**: ~500,000 packages

#### NPM (JavaScript/Node.js)
- **Method**: Changes feed (`_changes` endpoint)
- **Extraction**: Batch processing with `since` pagination (10,000 per batch)
- **Optimization**: Efficient incremental updates, set-based deduplication
- **Volume**: ~3,000,000 packages
- **Key Feature**: Real-time change tracking

#### Maven (Java)
- **Method**: Docker-based Lucene index extraction
- **Process**:
  1. Downloads Maven Central index (`nexus-maven-repository-index.gz` ~400-500 MB)
  2. Expands index using `indexer-cli` tool (~10-15 minutes)
  3. Reads Lucene index with PyLucene inside Docker container
  4. Extracts unique `groupId:artifactId` combinations
  5. Returns deduplicated package list via JSON stdout
- **Docker Image**: `coady/pylucene:9` with Java 17
- **Container**: Ephemeral (runs with `--rm`, auto-cleanup)
- **Duration**: 80-90 minutes per execution
- **Optimization**: Set-based deduplication (10M artifacts → ~500k-1M unique packages)
- **Volume**: ~500,000-1,000,000 unique packages
- **No Volumes**: All processing happens inside temporary container

#### NuGet (.NET)
- **Method**: Catalog API (`catalog0/index.json`)
- **Extraction**: Parallel processing of catalog pages with semaphore (50 concurrent)
- **Optimization**: `asyncio.gather()` for concurrent page fetching
- **Volume**: ~400,000 packages
- **Key Feature**: Paginated catalog with timestamp-based ordering

#### Cargo (Rust)
- **Method**: Git index cloning (`rust-lang/crates.io-index`)
- **Extraction**: Parse individual JSON files per crate
- **Optimization**: Shallow clone (`--depth=1`) to minimize download
- **Volume**: ~150,000 crates

#### RubyGems (Ruby)
- **Method**: Single request to names index
- **Extraction**: Plain text file with one gem per line
- **Optimization**: Single HTTP request fetches all gem names (~180k)
- **Volume**: ~180,000 gems
- **Endpoint**: `https://index.rubygems.org/names`

#### Go (Go modules)
- **Method**: Cursor-based streaming from `index.golang.org`
- **Extraction**: NDJSON feed with `?since=<timestamp>` pagination (2,000 per batch)
- **Optimization**: Pseudo-version filtering (only tagged releases processed unless no tags exist); latest-version-only go.mod resolution reduces transitive dependency work from O(versions) to O(1) per package; cycle guard via `_IN_PROGRESS` set prevents re-entrant calls on circular dependency graphs
- **Volume**: ~500,000+ modules (index grows continuously)
- **Key Feature**: Fully resumable — cursor persisted in Redis; safe to interrupt and restart at any point
- **Dependency resolution**: Parses `require` blocks from `go.mod` files fetched via `proxy.golang.org/@v/{version}.mod`
- **Endpoints**: `index.golang.org/index` (discovery) + `proxy.golang.org` (versions, go.mod, zip)

### Import Names Extraction

All packages automatically extract **import_names** - the list of modules, classes, or namespaces that can be imported from each package. This enables dependency analysis and usage pattern detection.

#### Extraction Strategy by Ecosystem

| Ecosystem | File Format | Extraction Method | Example Output |
|-----------|-------------|-------------------|----------------|
| **Cargo** | `.tar.gz` → `.rs` files | Regex for `pub mod/struct/enum/trait/fn/const/static/macro/type` | `["serde", "serde::Serialize", "serde::Deserialize"]` |
| **Maven** | `.jar` (ZIP) → `.class` files | Java package paths with deduplication | `["org.springframework.boot", "org.springframework.web"]` |
| **NPM** | `.tgz` → `.js/.mjs/.ts` files | Module path mapping, excludes tests | `["express", "express/lib/router", "express/lib/application"]` |
| **RubyGems** | `.gem` → `data.tar.gz` → `lib/*.rb` | Ruby module paths, converts to `::` format | `["rails", "rails::application", "rails::engine"]` |
| **NuGet** | `.nupkg` (ZIP) → `.dll` files | DLL name extraction + `.nuspec` parsing | `["Newtonsoft.Json", "System.Text.Json"]` |
| **PyPI** | `.whl` or `.tar.gz` → `.py` files | Python module discovery from `__init__.py` | `["requests", "requests.api", "requests.models"]` |
| **Go** | `.zip` (proxy) → `.go` files | Directory enumeration excluding `_test.go`; each dir with a `.go` file becomes an import path | `["github.com/gin-gonic/gin", "github.com/gin-gonic/gin/binding"]` |

## Useful Commands

### Local Development (UV)

```bash
# Install dependencies
uv sync

# Add new dependency
uv add <package-name>

# Add development dependency
uv add --dev <package-name>

# Remove dependency
uv remove <package-name>

# Run Dagster locally
uv run dagster dev -m src.dagster_app

# Run tests
uv run pytest

# Run linter
uv run ruff check src/

# Format code
uv run ruff format src/

# List installed packages
uv pip list

# Update dependencies
uv sync --upgrade
```

### Docker Services

```bash
# Build and start services
docker compose up -d --build

# View all service status
docker compose ps

# View logs from all services
docker compose logs -f

# View logs from specific service
docker compose logs -f dagster-webserver
docker compose logs -f dagster-daemon
docker compose logs -f redis

# Restart all services
docker compose restart

# Restart specific service
docker compose restart redis

# Stop services (keep data)
docker compose down

# Stop and remove all data (including Redis queue)
docker compose down -v
```

### Redis Service Commands

```bash
# Access Redis CLI inside container
docker compose exec redis redis-cli

# Check Redis is running
docker compose exec redis redis-cli ping
# Should return: PONG

# Monitor Redis in real-time
docker compose exec redis redis-cli MONITOR

# Check Redis memory usage
docker compose exec redis redis-cli INFO memory

# View all keys in Redis
docker compose exec redis redis-cli KEYS '*'

# Inspect or reset the Go ingestion cursor
docker compose exec redis redis-cli GET go_ingestion_cursor
docker compose exec redis redis-cli DEL go_ingestion_cursor  # resets to launch date on next run
```

### Running Assets

```bash
# Materialize a specific update asset
docker compose exec dagster-webserver \
  dagster asset materialize -m src.dagster_app -a pypi_packages_updates

# Run a bulk ingestion asset (Warning: can take hours!)
docker compose exec dagster-webserver \
  dagster asset materialize -m src.dagster_app -a pypi_package_ingestion

# Run Go ingestion (resumes from last cursor)
docker compose exec dagster-webserver \
  dagster asset materialize -m src.dagster_app -a go_package_ingestion

# Process Redis queue manually
docker compose exec dagster-webserver \
  dagster asset materialize -m src.dagster_app -a redis_queue_processor

# List all available assets
docker compose exec dagster-webserver \
  dagster asset list -m src.dagster_app

# View schedule status
docker compose exec dagster-webserver \
  dagster schedule list -m src.dagster_app
```

### Redis Queue Operations

```bash
# All commands run inside the Redis container

# Check number of messages in queue
docker compose exec redis redis-cli XLEN package-extraction

# Check number of messages in dead-letter queue
docker compose exec redis redis-cli XLEN package-extraction-dlq

# View consumer group info
docker compose exec redis redis-cli XINFO GROUPS package-extraction

# Add a test message to queue
docker compose exec redis redis-cli XADD package-extraction '*' data '{"node_type":"PyPIPackage","package":"requests","vendor":"Kenneth Reitz"}'

# Add a Go package message to queue
docker compose exec redis redis-cli XADD package-extraction '*' data '{"node_type":"GoPackage","package":"github.com/gin-gonic/gin"}'

# Read messages from dead-letter queue
docker compose exec redis redis-cli XREAD COUNT 10 STREAMS package-extraction-dlq 0

# View pending messages in consumer group
docker compose exec redis redis-cli XPENDING package-extraction extractors

# Clear all messages from queue (be careful!)
docker compose exec redis redis-cli DEL package-extraction

# Clear dead-letter queue
docker compose exec redis redis-cli DEL package-extraction-dlq
```

### Development

```bash
# Access webserver container
docker compose exec dagster-webserver bash

# Rebuild images after code changes
docker compose up -d --build

# Run Python shell in container
docker compose exec dagster-webserver python

# Test imports
docker compose exec dagster-webserver \
  python -c "from src.dagster_app import defs; print('OK')"
```

## Project Structure

```
securechain-ssc-ingestion/
├── dagster_home/                    # Dagster runtime configuration
│   ├── dagster.yaml                 # PostgreSQL storage, launchers, coordinators
│   └── workspace.yaml               # Module loading: src.dagster_app
├── src/
│   ├── __init__.py
│   ├── dagster_app/                 # Dagster application layer
│   │   ├── __init__.py              # Exports `defs` (Definitions with assets + schedules)
│   │   ├── assets/                  # Dagster asset definitions
│   │   │   ├── __init__.py          # Imports and re-exports all assets
│   │   │   ├── pypi_assets.py       # PyPI ingestion + updates
│   │   │   ├── npm_assets.py        # NPM ingestion + updates
│   │   │   ├── maven_assets.py      # Maven ingestion + updates
│   │   │   ├── nuget_assets.py      # NuGet ingestion + updates
│   │   │   ├── cargo_assets.py      # Cargo ingestion + updates
│   │   │   ├── rubygems_assets.py   # RubyGems ingestion + updates
│   │   │   ├── go_assets.py         # Go ingestion + updates (cursor-based)
│   │   │   └── redis_queue_assets.py # Redis stream queue processor
│   │   └── schedules.py             # 15 schedules (7 ingestion + 7 updates + 1 queue)
│   ├── processes/                   # Business logic (Dagster-agnostic)
│   │   ├── extractors/              # Package creation + dependency resolution
│   │   │   ├── __init__.py
│   │   │   ├── base.py              # Abstract PackageExtractor base class
│   │   │   ├── pypi_extractor.py
│   │   │   ├── npm_extractor.py
│   │   │   ├── maven_extractor.py
│   │   │   ├── nuget_extractor.py
│   │   │   ├── cargo_extractor.py
│   │   │   ├── rubygems_extractor.py
│   │   │   └── go_extractor.py      # Depth-capped + cycle-guarded
│   │   └── updaters/                # Version synchronisation per ecosystem
│   │       ├── __init__.py
│   │       ├── pypi_version_updater.py
│   │       ├── npm_version_updater.py
│   │       ├── maven_version_updater.py
│   │       ├── nuget_version_updater.py
│   │       ├── cargo_version_updater.py
│   │       ├── rubygems_version_updater.py
│   │       └── go_version_updater.py
│   ├── services/                    # External service clients
│   │   ├── __init__.py
│   │   ├── apis/                    # Registry HTTP clients
│   │   │   ├── __init__.py
│   │   │   ├── pypi_service.py
│   │   │   ├── npm_service.py
│   │   │   ├── maven_service.py
│   │   │   ├── nuget_service.py
│   │   │   ├── cargo_service.py
│   │   │   ├── rubygems_service.py
│   │   │   └── go_service.py        # index.golang.org + proxy.golang.org
│   │   ├── graph/                   # Neo4j graph database layer
│   │   │   ├── __init__.py
│   │   │   ├── package_service.py   # CRUD for Package nodes
│   │   │   └── version_service.py   # CRUD for Version nodes
│   │   └── vulnerability/           # MongoDB CVE data layer
│   │       ├── __init__.py
│   │       └── vulnerability_service.py
│   ├── schemas/                     # Pydantic data models
│   │   ├── __init__.py
│   │   ├── pypi_package_schema.py
│   │   ├── npm_package_schema.py
│   │   ├── maven_package_schema.py
│   │   ├── nuget_package_schema.py
│   │   ├── cargo_package_schema.py
│   │   ├── rubygems_package_schema.py
│   │   ├── go_package_schema.py
│   │   └── package_message_schema.py # Redis message contract
│   ├── utils/                       # Shared utilities
│   │   ├── __init__.py
│   │   ├── attributor.py            # CVE attribution for versions
│   │   ├── orderer.py               # Semantic version sorting
│   │   ├── redis_queue.py           # Redis stream read/ack/DLQ
│   │   ├── repo_normalizer.py       # Repository URL normalisation
│   │   ├── pypi_constraints_parser.py
│   │   └── maven/                   # Maven-specific extraction tooling
│   │       ├── Dockerfile.maven     # coady/pylucene:9 + Java 17
│   │       └── automate_maven_extraction.py
│   ├── __init__.py
│   ├── cache.py                     # aiocache in-memory cache manager
│   ├── database.py                  # Neo4j + MongoDB connection pool (singleton)
│   ├── dependencies.py              # ServiceContainer + module-level getters
│   ├── logger.py                    # Rotating file logger (singleton)
│   ├── session.py                   # aiohttp ClientSession manager
│   └── settings.py                  # Pydantic Settings (reads .env)
├── tests/
│   └── unit/
│       ├── extractors/
│       │   └── test_go_extractor.py
│       ├── schemas/
│       │   └── test_go_package_schema.py
│       ├── services/
│       │   └── test_go_service.py
│       └── updaters/
│           └── test_go_version_updater.py
├── docker-compose.yml               # 4 services: postgres, redis, daemon, webserver
├── Dockerfile                       # Multi-stage UV build
├── pyproject.toml                   # Dependencies + Ruff + Hatch config
├── .env.template                    # Environment variable template
├── .env                             # Local configuration (gitignored)
├── .github/
│   └── workflows/
│       ├── lint-test.yml            # Ruff linter on push/PR
│       └── release.yml              # Multi-arch Docker image + GitHub Release on tag
└── CLAUDE.md                        # AI agent context documentation
```

## Configuration

### Environment Variables

Copy `.env.template` to `.env` and configure the following sections:

#### Database Connections
```bash
# Neo4j (Graph database)
GRAPH_DB_URI='bolt://neo4j:7687'
GRAPH_DB_USER='neo4j'
GRAPH_DB_PASSWORD='your-secure-password'  # Change in production!

# MongoDB (Vulnerability database)
VULN_DB_URI='mongodb://mongoSecureChain:mongoSecureChain@mongo:27017/admin'
```

#### Redis Queue Configuration

Redis is used for asynchronous package extraction and runs as a Docker service. The `redis_queue_processor` asset consumes messages from the stream every 5 minutes. Redis also stores the Go ingestion cursor under the key `go_ingestion_cursor`.

```bash
REDIS_HOST=redis                  # Redis service name in docker-compose
REDIS_PORT=6379                   # Redis server port
REDIS_DB=0                        # Redis database number
REDIS_STREAM=package-extraction   # Stream name for messages
REDIS_GROUP=extractors            # Consumer group name
REDIS_CONSUMER=package-consumer   # Consumer identifier (generic, not ecosystem-specific)
```

**Redis Features**:
- ✅ Runs as Docker service (redis:7-alpine)
- ✅ Data persistence with AOF (Append Only File)
- ✅ Health checks enabled
- ✅ Volume mount for data persistence (`redis_data`)
- ✅ Exposed on port 6379 for external access if needed
- ✅ Stores Go ingestion cursor for resumable incremental ingestion

**Note**: The consumer name was changed from `pypi-consumer` to `package-consumer` to reflect support for all 7 package ecosystems.

#### Dagster PostgreSQL
```bash
POSTGRES_USER=dagster
POSTGRES_PASSWORD=your-secure-password  # Change in production!
POSTGRES_DB=dagster
POSTGRES_HOST=dagster-postgres
POSTGRES_PORT=5432
```

#### Application Settings
```bash
DAGSTER_HOME=/opt/dagster/dagster_home
PYTHONPATH=/opt/dagster/app
```

All configuration is managed through the `Settings` class in `src/settings.py` using Pydantic Settings for validation and type safety.

## Performance Considerations

### Ingestion Assets
- **Duration**: Each ingestion can take several hours depending on registry size
- **Network**: Requires stable internet connection
- **Rate Limiting**: Built-in delays to respect API limits
- **Memory**: Maven deduplication uses set-based approach for efficiency
- **Caching**: 1-hour TTL on package listings to reduce API calls
- **Go**: Cursor-based batching (2,000 modules/batch) avoids loading the full index in memory; pseudo-version filtering and latest-version-only go.mod resolution significantly reduce network calls

### Update Assets
- **Duration**: Typically completes in minutes to hours
- **Batch Size**: Processes packages in configurable batches
- **Concurrency**: Async operations for improved throughput
- **Error Handling**: Individual package failures don't stop the entire run

## Monitoring

### Dagster UI (http://localhost:3000)

Access real-time monitoring and management:

- **Assets**: View materialization history, metadata, and dependencies
- **Runs**: Monitor active runs, view logs, inspect failures
- **Schedules**: Enable/disable schedules, view next execution times
- **Sensors**: (Future) Event-driven execution triggers
- **Dagit Logs**: Detailed execution traces with timestamps

### Metrics Tracked

Each asset reports comprehensive metrics:

**Ingestion Assets** (PyPI, NPM, Maven, NuGet, Cargo, RubyGems):
- `total_in_registry`: Total packages found in registry
- `new_packages_ingested`: New packages added to graph
- `skipped_existing`: Packages already in graph
- `errors`: Failed ingestions
- `ingestion_rate`: Percentage of new packages

**Go Ingestion Asset** (cursor-based, different metric set):
- `total_scanned`: Total modules scanned across all index batches
- `new_packages_ingested`: New modules added to graph
- `skipped_existing`: Modules already in graph
- `errors`: Failed ingestions
- `cursor_start`: Timestamp cursor at the start of the run
- `cursor_end`: Timestamp cursor at the end of the run (persisted in Redis)

**Update Assets**:
- `packages_processed`: Total packages updated
- `total_versions`: New versions added
- `errors`: Failed updates
- `success_rate`: Percentage of successful updates

**Redis Queue Processor**:
- `total_processed`: Total messages read from queue
- `successful`: Successfully processed messages
- `failed`: Failed processing (moved to DLQ)
- `validation_errors`: Messages with invalid schema
- `unsupported_types`: Messages with unknown node_type
- `success_rate`: Percentage of successful processing

## Troubleshooting

### Common Issues

**Services won't start**
```bash
# Check logs
docker compose logs dagster-daemon
docker compose logs dagster-webserver
docker compose logs redis

# Verify network exists
docker network inspect securechain

# Check all services status
docker compose ps

# Rebuild containers
docker compose up -d --build
```

**Redis not starting**
```bash
# Check Redis logs
docker compose logs redis

# Verify Redis health
docker compose exec redis redis-cli ping

# Restart Redis
docker compose restart redis

# Remove Redis data and restart (WARNING: clears all messages AND Go cursor)
docker compose down -v
docker compose up -d
```

**Assets not appearing in UI**
```bash
# Verify imports
docker compose exec dagster-webserver \
  python -c "from src.dagster_app import defs; print(len(defs.get_asset_graph().get_all_asset_keys()))"

# Should print 15 (7 ingestion + 7 update + 1 queue processor)
```

**Database connection errors**
```bash
# Check .env file matches docker-compose.yml service names
# For dockerized: use service names (neo4j, mongo, dagster-postgres)
# For local: use localhost
```

**Redis connection errors**
```bash
# Check Redis is running and accessible from webserver
docker compose exec dagster-webserver python -c "from redis import Redis; r = Redis(host='redis', port=6379); print('Redis OK:', r.ping())"

# Verify Redis service is in same network
docker network inspect securechain | grep redis

# Check Redis configuration in .env
cat .env | grep REDIS

# If consumer group doesn't exist, create it:
docker compose exec redis redis-cli XGROUP CREATE package-extraction extractors 0 MKSTREAM

# Check consumer group status
docker compose exec redis redis-cli XINFO GROUPS package-extraction
```

**Go ingestion not advancing / cursor issues**
```bash
# Check current cursor value
docker compose exec redis redis-cli GET go_ingestion_cursor

# Reset cursor to restart ingestion from the beginning
docker compose exec redis redis-cli DEL go_ingestion_cursor

# Force cursor to a specific date (RFC3339 format)
docker compose exec redis redis-cli SET go_ingestion_cursor "2024-01-01T00:00:00Z"
```

**Messages stuck in dead-letter queue**
```bash
# Check DLQ length
docker compose exec redis redis-cli XLEN package-extraction-dlq

# Read messages from DLQ
docker compose exec redis redis-cli XREAD COUNT 10 STREAMS package-extraction-dlq 0

# Clear DLQ if needed (after fixing issues)
docker compose exec redis redis-cli DEL package-extraction-dlq
```

**Port 3000 already in use**
```yaml
# In docker-compose.yml, change port mapping:
ports:
  - "3001:3000"  # Access UI at http://localhost:3001
```

**Redis consumer group errors**
```bash
# If you changed REDIS_CONSUMER, recreate the group:
redis-cli XGROUP DESTROY package-extraction extractors
redis-cli XGROUP CREATE package-extraction extractors 0 MKSTREAM
```

## Development Workflow

### Making Changes

1. **Edit code** in `src/` directory
2. **Rebuild containers**: `docker compose up -d --build`
3. **Verify in UI**: Check assets appear at http://localhost:3000
4. **Test manually**: Materialize asset to verify behavior
5. **Monitor logs**: Watch for errors in webserver logs

### Adding New Package Ecosystem

See `CLAUDE.md` for detailed instructions on adding support for new package registries. Note: `CLAUDE.md` predates the Go integration and still references 6 ecosystems and 12 assets — refer to the actual source files for the current state. Summary:

1. Create API service in `src/services/apis/`
2. Create schema in `src/schemas/`
3. Create extractor in `src/processes/extractors/`
4. Create updater in `src/processes/updaters/`
5. Create assets in `src/dagster_app/assets/`
6. Add schedules in `src/dagster_app/schedules.py`
7. Update imports in `__init__.py` files
8. Add extractor mapping in `redis_queue_assets.py` for queue processing
9. Register service getter in `src/dependencies.py`

### Working with Redis Queue

To add a message to the queue for processing:

```python
import json
from redis import Redis

r = Redis(host='localhost', port=6379, db=0)

# Create message following PackageMessageSchema
message = {
    "node_type": "PyPIPackage",      # Required
    "package": "requests",            # Required
    "vendor": "Kenneth Reitz",        # Optional
    "repository_url": "https://github.com/psf/requests",  # Optional
    "constraints": ">=2.0.0,<3.0.0",  # Optional
    "parent_id": "abc123",            # Optional
    "parent_version": "1.0.0",        # Optional
    "refresh": False                  # Optional
}

# Add to stream
r.xadd("package-extraction", {"data": json.dumps(message)})

# Go package example
go_message = {
    "node_type": "GoPackage",
    "package": "github.com/gin-gonic/gin",
}
r.xadd("package-extraction", {"data": json.dumps(go_message)})
```

The `redis_queue_processor` will pick up the message in the next run (every 5 minutes).

## Testing

The project uses **pytest** with **pytest-asyncio** for unit tests. Tests are located in `tests/unit/` and cover the Go ecosystem implementation. All other ecosystem tests follow the same patterns.

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/unit/services/test_go_service.py -v

# Run a specific test
uv run pytest tests/unit/extractors/test_go_extractor.py::test_cycle_protection_stops_infinite_recursion -v

# Run tests in Docker
docker compose exec dagster-webserver uv run pytest tests/
```

### Test Structure

```
tests/
└── unit/
    ├── extractors/
    │   └── test_go_extractor.py      # GoPackageExtractor: run, cycle protection,
    │                                  # dependency resolution, latest-version-only logic
    ├── schemas/
    │   └── test_go_package_schema.py  # GoPackageSchema: defaults, to_dict, whitespace
    ├── services/
    │   └── test_go_service.py         # GoService: NDJSON parsing, version fetching,
    │                                  # cursor pagination, go.mod parsing, import names
    └── updaters/
        └── test_go_version_updater.py # GoVersionUpdater: delta detection, version sync
```

### CI

Tests and linting run automatically on every push and pull request via GitHub Actions (`.github/workflows/lint-test.yml`). The workflow runs **Ruff** for linting and **pytest** for tests using Python 3.13 and UV.

## Contributing

Pull requests are welcome! To contribute follow this [guidelines](https://securechaindev.github.io/contributing.html).

## License
[GNU General Public License 3.0](https://www.gnu.org/licenses/gpl-3.0.html)

## Links
- [Secure Chain Team](mailto:hi@securechain.dev)
- [Secure Chain Organization](https://github.com/securechaindev)
- [Secure Chain Documentation](https://securechaindev.github.io/)