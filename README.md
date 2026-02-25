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
- 🔀 **Duplicate Indexes** - Redundant indexes wasting space
- 🔗 **FK Missing Indexes** - Foreign keys without indexes (slow JOINs)
- ⏳ **Transaction ID Age** - Tables approaching wraparound
- 🛡️ **Security Checks** - Public schema permissions audit
- 💾 **Tablespace Usage** - Tablespace sizes and locations
- 🔀 **Replication Slots** - Slot status and WAL retention
- 📝 **Background Writer** - Checkpoint statistics
- 📊 **WAL Statistics** - WAL file count and settings
- ⚙️ **Configuration Audit** - Settings vs best practices
- 📱 **Notifications** - Telegram, Slack, Webhook alerts

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

# Get actionable recommendations
pg-health suggest -c "..."

# Apply quick fixes (with dry-run)
pg-health fix unused-indexes -c "..." --dry-run
pg-health fix vacuum -c "..." --tables orders,users
pg-health fix all -c "..."

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

## MCP Server (for AI Agents)

pg-health includes an MCP server for integration with Claude, Cursor, and other AI tools.

### Setup

```bash
pip install fastmcp
```

### Add to Claude Desktop

Add to `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pg-health": {
      "command": "pg-health-mcp"
    }
  }
}
```

### Available Tools

| Tool | Description |
|------|-------------|
| `pg_health_check` | Run comprehensive health check, returns status + issues |
| `pg_health_suggest` | Get prioritized optimization recommendations |
| `pg_health_fix` | Apply fixes (dry-run by default for safety) |

### Example Usage

Claude or other agents can:

```
> Check my database health
[uses pg_health_check with your connection string]

> What should I optimize?
[uses pg_health_suggest, returns prioritized recommendations]

> Drop the unused indexes (dry run first)
[uses pg_health_fix with fix_type="unused-indexes", dry_run=True]
```

### Direct Python Usage

```python
from pg_health.mcp_server import pg_health_check, pg_health_suggest, pg_health_fix

# Get health report as JSON
result = pg_health_check("postgresql://user:pass@host:5432/db")

# Get recommendations
suggestions = pg_health_suggest("postgresql://...")

# Preview fixes (dry run)
fixes = pg_health_fix("postgresql://...", fix_type="unused-indexes", dry_run=True)
```

---

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

### Auto-Suggest

Get actionable recommendations based on your database health:

```bash
pg-health suggest -c "postgresql://user:pass@host:5432/db"
```

Output:
```
🔍 Analyzing database health...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Recommendations
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔴 HIGH PRIORITY

1. Increase shared_buffers
   Why: Cache hit ratio is 89.0% (should be >95%)
   Impact: Better cache hit ratio means faster queries
   Action: Edit postgresql.conf, set shared_buffers to ~25% of RAM. Current: 128MB

2. VACUUM ANALYZE public.orders
   Why: 150,000 dead tuples (15.2% bloat)
   Impact: Reclaim disk space, improve query performance
   SQL: VACUUM ANALYZE public.orders;

🟡 MEDIUM PRIORITY

3. Drop unused index idx_old_column
   Why: 0 scans since stats reset, 50MB wasted
   Impact: Free 50MB disk space, faster writes
   SQL: DROP INDEX public.idx_old_column;

🟢 LOW PRIORITY

4. Consider partitioning public.logs
   Why: Table is 12GB with 50,000,000 rows
   Impact: Improved query performance, easier maintenance
   Action: Partition by date/time column if available
```

Recommendations include:
- **Cache tuning** - shared_buffers optimization when cache hit ratio is low
- **Unused indexes** - Indexes that waste space and slow writes
- **Vacuum suggestions** - Tables with high dead tuple counts
- **Missing indexes** - Tables with heavy sequential scans
- **Partitioning hints** - Large tables that could benefit from partitioning
- **Statistics updates** - Tables with outdated statistics
- **Slow query analysis** - Via pg_stat_statements

### Quick Fix

Apply fixes automatically or preview with `--dry-run`:

```bash
# Preview what would be dropped
pg-health fix unused-indexes -c "..." --dry-run

# Actually drop unused indexes
pg-health fix unused-indexes -c "..."

# Vacuum specific tables
pg-health fix vacuum -c "..." --tables orders,users

# Run all safe fixes
pg-health fix all -c "..." --dry-run
```

**Safe fixes** (can auto-execute):
| Fix Type | What it does |
|----------|--------------|
| `unused-indexes` | DROP INDEX for indexes with 0 scans |
| `vacuum` | VACUUM ANALYZE tables with high dead tuples |
| `analyze` | ANALYZE tables with outdated statistics |
| `all` | Run all of the above |

**Unsafe operations** (suggest only shows SQL, never executes):
- CREATE INDEX (may lock table)
- Config changes (requires restart)
- Schema changes

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
| Duplicate Indexes | 0 | 1+ pairs | - |
| FK Missing Indexes | 0-3 | 4+ | - |
| Transaction ID Age | < 500M | 500M-1B | > 1B |

All thresholds are configurable via the YAML config file.

## Requirements

- Python 3.11+
- PostgreSQL 12+ (for pg_stat_statements)
- PyYAML (for custom thresholds)

## Cloud Database Support

### Supabase

```bash
# Get connection string from Supabase Dashboard > Settings > Database
pg-health check -c "postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres"

# Or use direct connection (port 5432)
pg-health check -c "postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres"
```

### Neon

```bash
# Get connection string from Neon Console
pg-health check -c "postgresql://[user]:[password]@[endpoint].neon.tech/[database]?sslmode=require"
```

### AWS RDS / Aurora

```bash
pg-health check -c "postgresql://[user]:[password]@[instance].rds.amazonaws.com:5432/[database]"
```

**Note:** Some checks (like pg_stat_statements) may require enabling extensions or specific permissions on managed databases.

## Privacy

Your connection string is never stored. All checks run in real-time and results are not saved on our servers.

## Notifications

Get alerts when health checks find issues.

### Telegram

```bash
# Set credentials
export PG_HEALTH_TELEGRAM_TOKEN="your-bot-token"
export PG_HEALTH_TELEGRAM_CHAT_ID="your-chat-id"

# Run check and notify
pg-health notify -c "postgresql://..." --provider telegram

# Only notify if there are issues (default)
pg-health notify -c "..." --only-issues

# Always notify (even if healthy)
pg-health notify -c "..." --always
```

### Slack

```bash
export PG_HEALTH_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
pg-health notify -c "..." --provider slack
```

### Webhook (generic)

```bash
export PG_HEALTH_WEBHOOK_URL="https://your-server.com/webhook"
pg-health notify -c "..." --provider webhook
```

Webhook payload:
```json
{
  "database": "mydb",
  "status": "warning",
  "has_issues": true,
  "checks": [...],
  "summary": {"total_checks": 20, "warnings": 2, "criticals": 0}
}
```

### Cron Example

```bash
# Run every hour, only alert on issues
0 * * * * pg-health notify -c "$DATABASE_URL" --only-issues
```

### Email

```bash
export PG_HEALTH_SMTP_HOST="smtp.gmail.com"
export PG_HEALTH_SMTP_PORT="587"
export PG_HEALTH_SMTP_USER="your@gmail.com"
export PG_HEALTH_SMTP_PASS="app-password"
export PG_HEALTH_EMAIL_FROM="your@gmail.com"
export PG_HEALTH_EMAIL_TO="alerts@yourcompany.com"

pg-health notify -c "..." --provider email
```

## Historical Trends

Track health metrics over time with built-in SQLite storage.

```bash
# Run check and save to history
pg-health check -c "..." --save

# View history
pg-health history
pg-health history --database mydb --days 30

# List available metrics
pg-health trend mydb

# View specific metric trend
pg-health trend mydb --metric "Cache Hit Ratio.ratio"
pg-health trend mydb --metric "Connection Usage.usage_ratio" --days 30
```

Data is stored in `~/.pg-health/history.db` (configurable via `PG_HEALTH_DATA_DIR`).

Available metrics include:
- Cache/Index hit ratios
- Connection counts
- Lock waits
- Background writer stats
- Database size

## Roadmap

- [x] Replication lag monitoring
- [x] Lock wait detection
- [x] Vacuum statistics
- [x] Configurable thresholds
- [x] JSON output mode
- [x] Status badges
- [x] Auto-suggest recommendations
- [x] Quick fix commands
- [x] Telegram notifications
- [x] Slack notifications
- [x] Webhook notifications
- [x] Email alerts (SMTP)
- [x] Historical trends (SQLite)
- [x] Supabase/Neon support (via connection string)
- [ ] Scheduled checks (built-in daemon)

## License

MIT
