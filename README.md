# PG Health 🐘

PostgreSQL health check and optimization tool. Get instant insights into your database's health, performance, and potential issues.

## Features

- 🔍 **Cache Hit Ratio** - Are you reading from memory or disk?
- 📊 **Connection Usage** - How close to max_connections?
- ⏱️ **Long Running Queries** - Queries running > 5 minutes
- 📦 **Unused Indexes** - Wasting space and slowing writes
- 🗑️ **Table Bloat** - Dead tuples that need vacuuming
- 🔑 **Missing Primary Keys** - Tables without PKs
- 🐢 **Slow Queries** - Via pg_stat_statements
- 🔄 **Replication Lag** - Time behind primary (replicas)
- 🔒 **Lock Waits** - Queries waiting for locks
- 🧹 **Vacuum Stats** - Tables needing vacuum attention

## Quick Start

```bash
# Install
cd pg-health
pip install -e .

# CLI - Run health check
pg-health check -c "postgresql://user:pass@host:5432/dbname"

# Web UI
pg-health serve
# Open http://localhost:8767
```

## CLI Usage

```bash
# Basic check
pg-health check -c "postgresql://user:pass@localhost:5432/mydb"

# Save report to JSON file
pg-health check -c "..." -o report.json

# JSON output to stdout (agent-friendly)
pg-health check -c "..." --json

# Quiet mode - just output status (OK, WARNING, or CRITICAL)
pg-health check -c "..." --quiet

# With custom thresholds
pg-health check -c "..." --config config.yaml

# Start web interface
pg-health serve --port 8767

# Generate status badge
pg-health badge -c "..." -o badge.svg
```

### Exit Codes

The `check` command returns meaningful exit codes for automation:

| Exit Code | Status | Meaning |
|-----------|--------|---------|
| 0 | OK | All checks passed |
| 1 | WARNING | One or more warnings |
| 2 | CRITICAL | One or more critical issues |

### Agent-Friendly Output

For CI/CD pipelines and monitoring scripts:

```bash
# JSON output for parsing
pg-health check -c "..." --json | jq '.status'

# Simple status for shell scripts
if [ "$(pg-health check -c "..." --quiet)" = "OK" ]; then
  echo "Database healthy"
fi

# Use exit codes
pg-health check -c "..." --quiet
case $? in
  0) echo "OK" ;;
  1) echo "WARNING - check logs" ;;
  2) echo "CRITICAL - immediate attention needed" ;;
esac
```

### Status Badge

Generate an SVG badge for dashboards or READMEs:

```bash
# Output to file
pg-health badge -c "..." -o badge.svg

# Output to stdout (pipe to file or HTTP response)
pg-health badge -c "..."
```

Badge shows:
- **Green** "DB Health | OK" - All checks passed
- **Yellow** "DB Health | 2 warnings" - Warning-level issues
- **Red** "DB Health | CRITICAL" - Critical issues detected

## Configuration

### Threshold Configuration

Create a YAML config file to customize thresholds:

```yaml
# config.yaml
thresholds:
  cache_hit_ratio:
    warning: 0.95    # Warn if < 95%
    critical: 0.90   # Critical if < 90%
  
  connections:
    warning: 0.70    # Warn if > 70% of max_connections
    critical: 0.90
  
  replication_lag:
    warning: 10      # Warn if > 10 seconds behind
    critical: 60     # Critical if > 60 seconds behind
  
  dead_tuples:
    warning: 100000  # Warn if any table has > 100k dead tuples
    critical: 1000000
  
  lock_waits:
    warning: 5       # Warn if > 5 queries waiting for locks
    critical: 20
  
  table_bloat:
    warning: 0.10    # Warn if > 10% dead tuples
    critical: 0.20
```

Use the config:

```bash
# Via command line
pg-health check -c "..." --config config.yaml

# Via environment variable
export PG_HEALTH_CONFIG=/path/to/config.yaml
pg-health check -c "..."
```

See `config.example.yaml` for a complete example.

### Environment Variables

```bash
# Set connection string
DATABASE_URL=postgresql://user:password@host:5432/database

# Set config file path
PG_HEALTH_CONFIG=/path/to/config.yaml
```

## API Usage

```bash
# JSON API (AI-friendly)
curl -X POST https://pg.indiekit.ai/api/check \
  -H "Content-Type: application/json" \
  -d '{"connection_string": "postgresql://user:pass@host:5432/db"}'
```

Response:
```json
{
  "ok": true,
  "report": {
    "database_name": "mydb",
    "checks": [
      {"name": "Cache Hit Ratio", "severity": "ok", "message": "..."},
      {"name": "Replication Lag", "severity": "info", "message": "Not a replica"},
      {"name": "Lock Waits", "severity": "ok", "message": "0 waiting locks"},
      ...
    ],
    "unused_indexes": [...],
    "tables": [...],
    "slow_queries": [...],
    "vacuum_stats": [...]
  }
}
```

Note: Special characters in password (like `@`) are auto-encoded.

## Health Checks

| Check | OK | Warning | Critical |
|-------|-----|---------|----------|
| Cache Hit Ratio | > 95% | 90-95% | < 90% |
| Index Hit Ratio | > 95% | 90-95% | < 90% |
| Connection Usage | < 70% | 70-90% | > 90% |
| Replication Lag | < 10s | 10-60s | > 60s |
| Lock Waits | ≤ 5 | 6-20 | > 20 |
| Dead Tuples | < 100k | 100k-1M | > 1M |
| Table Bloat | < 10% dead | 10-20% dead | > 20% dead |

All thresholds are configurable via the YAML config file.

## Requirements

- Python 3.11+
- PostgreSQL 12+ (for pg_stat_statements)
- PyYAML (for custom thresholds)

## Privacy

Your connection string is never stored. All checks run in real-time and results are not saved on our servers.

## Roadmap

- [x] Replication lag monitoring
- [x] Lock wait detection
- [x] Vacuum statistics
- [x] Configurable thresholds
- [x] JSON output mode
- [x] Status badges
- [ ] Email alerts for critical issues
- [ ] Scheduled checks (cron)
- [ ] Historical trends
- [ ] Supabase/Neon integration

## License

MIT
