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

# Save report to JSON
pg-health check -c "..." -o report.json

# Start web interface
pg-health serve --port 8767
```

## Environment Variables

```bash
# Set connection string in .env
DATABASE_URL=postgresql://user:password@host:5432/database
```

## Health Checks

| Check | OK | Warning | Critical |
|-------|-----|---------|----------|
| Cache Hit Ratio | > 95% | 80-95% | < 80% |
| Index Hit Ratio | > 95% | 80-95% | < 80% |
| Connection Usage | < 70% | 70-90% | > 90% |
| Table Bloat | < 10% dead | 10-20% dead | > 20% dead |

## Requirements

- Python 3.11+
- PostgreSQL 12+ (for pg_stat_statements)

## Privacy

Your connection string is never stored. All checks run in real-time and results are not saved on our servers.

## Roadmap

- [ ] Email alerts for critical issues
- [ ] Scheduled checks (cron)
- [ ] Historical trends
- [ ] More checks (vacuum status, replication lag, etc.)
- [ ] Supabase/Neon integration

## License

MIT
