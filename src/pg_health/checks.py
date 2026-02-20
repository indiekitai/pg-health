"""PostgreSQL health check queries and logic."""

import re
from urllib.parse import quote
import asyncpg
from .models import (
    CheckResult,
    HealthConfig,
    Severity,
    TableInfo,
    IndexInfo,
    SlowQuery,
    HealthReport,
    VacuumInfo,
)


# SQL Queries for health checks
QUERIES = {
    "version": "SELECT version();",
    
    "database_size": """
        SELECT pg_database.datname,
               pg_size_pretty(pg_database_size(pg_database.datname)) as size
        FROM pg_database
        WHERE datname = current_database();
    """,
    
    "database_size_bytes": """
        SELECT pg_database_size(current_database()) as size_bytes;
    """,
    
    "table_sizes": """
        SELECT 
            schemaname as schema_name,
            tablename as table_name,
            pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) as table_size,
            pg_size_pretty(pg_indexes_size(schemaname || '.' || tablename)) as index_size,
            (SELECT reltuples::bigint FROM pg_class WHERE oid = (schemaname || '.' || tablename)::regclass) as row_count
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
        LIMIT 20;
    """,
    
    "unused_indexes": """
        SELECT 
            sui.schemaname as schema_name,
            sui.relname as table_name,
            sui.indexrelname as index_name,
            pg_size_pretty(pg_relation_size(sui.indexrelid)) as index_size,
            sui.idx_scan as index_scans
        FROM pg_stat_user_indexes sui
        JOIN pg_index pi ON sui.indexrelid = pi.indexrelid
        WHERE sui.idx_scan = 0
          AND NOT pi.indisprimary      -- exclude primary keys
          AND NOT pi.indisunique       -- exclude unique constraints
        ORDER BY pg_relation_size(sui.indexrelid) DESC
        LIMIT 20;
    """,
    
    "stats_reset": """
        SELECT stats_reset FROM pg_stat_database 
        WHERE datname = current_database();
    """,
    
    "duplicate_indexes": """
        SELECT 
            pg_size_pretty(sum(pg_relation_size(idx))::bigint) as size,
            array_agg(idx) as indexes
        FROM (
            SELECT indexrelid::regclass as idx, 
                   indrelid::regclass as tbl,
                   indkey as cols
            FROM pg_index
            WHERE indisunique = false
        ) sub
        GROUP BY tbl, cols
        HAVING count(*) > 1;
    """,
    
    "cache_hit_ratio": """
        SELECT 
            sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) as ratio
        FROM pg_statio_user_tables;
    """,
    
    "index_hit_ratio": """
        SELECT 
            sum(idx_blks_hit) / nullif(sum(idx_blks_hit) + sum(idx_blks_read), 0) as ratio
        FROM pg_statio_user_indexes;
    """,
    
    "connection_count": """
        SELECT count(*) as total,
               count(*) FILTER (WHERE state = 'active') as active,
               count(*) FILTER (WHERE state = 'idle') as idle,
               (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
        FROM pg_stat_activity
        WHERE datname = current_database();
    """,
    
    "long_running_queries": """
        SELECT pid, 
               now() - pg_stat_activity.query_start as duration,
               query,
               state
        FROM pg_stat_activity
        WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
          AND state != 'idle'
          AND query NOT ILIKE '%pg_stat_activity%';
    """,
    
    "bloat_estimate": """
        SELECT 
            schemaname || '.' || relname as table_name,
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) as table_size,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 2) as dead_ratio
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 1000
        ORDER BY n_dead_tup DESC
        LIMIT 10;
    """,
    
    "slow_queries": """
        SELECT query,
               calls,
               total_exec_time as total_time_ms,
               mean_exec_time as mean_time_ms,
               rows
        FROM pg_stat_statements
        WHERE calls > 10
        ORDER BY mean_exec_time DESC
        LIMIT 10;
    """,
    
    "missing_primary_keys": """
        SELECT n.nspname as schema_name, c.relname as table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (
              SELECT 1 FROM pg_constraint con
              WHERE con.conrelid = c.oid AND con.contype = 'p'
          );
    """,
    
    "tables_without_indexes": """
        SELECT schemaname || '.' || relname as table_name,
               seq_scan,
               idx_scan
        FROM pg_stat_user_tables
        WHERE idx_scan = 0 AND seq_scan > 100
        ORDER BY seq_scan DESC
        LIMIT 10;
    """,
    
    # New queries for additional checks
    "replication_lag": """
        SELECT CASE WHEN pg_is_in_recovery() THEN 
            EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::int 
        ELSE NULL END as lag_seconds;
    """,
    
    "vacuum_stats": """
        SELECT schemaname, relname, n_dead_tup, last_vacuum, last_autovacuum
        FROM pg_stat_user_tables 
        WHERE n_dead_tup > 10000 
        ORDER BY n_dead_tup DESC 
        LIMIT 10;
    """,
    
    "lock_waits": """
        SELECT count(*) as waiting_locks FROM pg_locks WHERE NOT granted;
    """,
    
    "disk_usage": """
        SELECT pg_database_size(current_database()) as db_size_bytes;
    """,
}


def fix_connection_string(connection_string: str) -> str:
    """Fix special characters in password that need URL encoding.
    
    Handles cases like: postgresql://user:pass@word@host/db
    where the password contains @ or other special chars.
    """
    # Match postgresql://user:password@host:port/database
    # The trick: find the LAST @ before the host (which doesn't contain @)
    match = re.match(
        r'^(postgresql://|postgres://)([^:]+):(.+)@([^@/]+)(/.*)$',
        connection_string
    )
    if match:
        scheme, user, password, host, path = match.groups()
        # URL-encode the password
        encoded_password = quote(password, safe='')
        return f"{scheme}{user}:{encoded_password}@{host}{path}"
    
    # If pattern doesn't match, return as-is
    return connection_string


def format_bytes(bytes_val: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} PB"


async def run_health_check(
    connection_string: str, 
    config: HealthConfig | None = None
) -> HealthReport:
    """Run all health checks and return a report."""
    
    if config is None:
        config = HealthConfig.defaults()
    
    # Fix special characters in password
    connection_string = fix_connection_string(connection_string)
    
    conn = await asyncpg.connect(connection_string)
    
    try:
        # Get basic info
        version = await conn.fetchval(QUERIES["version"])
        db_info = await conn.fetchrow(QUERIES["database_size"])
        
        report = HealthReport(
            database_name=db_info["datname"],
            database_version=version.split(",")[0] if version else "Unknown",
        )
        
        # Check: Database size (with human-readable format)
        db_size_bytes = await conn.fetchval(QUERIES["database_size_bytes"])
        report.checks.append(CheckResult(
            name="Database Size",
            description="Total database size",
            severity=Severity.INFO,
            message=f"Database size: {db_info['size']}",
            details={"size_bytes": db_size_bytes, "size_pretty": db_info['size']},
        ))
        
        # Check: Replication Lag (for replicas)
        lag_seconds = await conn.fetchval(QUERIES["replication_lag"])
        if lag_seconds is not None:
            threshold = config.get_threshold("replication_lag")
            if lag_seconds > threshold.critical:
                severity = Severity.CRITICAL
            elif lag_seconds > threshold.warning:
                severity = Severity.WARNING
            else:
                severity = Severity.OK
            
            report.checks.append(CheckResult(
                name="Replication Lag",
                description="Time behind primary (replica only)",
                severity=severity,
                message=f"Replication lag: {lag_seconds}s",
                details={"lag_seconds": lag_seconds},
                suggestion="Check network/disk I/O on replica" if severity != Severity.OK else None,
            ))
        else:
            report.checks.append(CheckResult(
                name="Replication Lag",
                description="Time behind primary (replica only)",
                severity=Severity.INFO,
                message="Not a replica (primary server)",
                details={"is_replica": False},
            ))
        
        # Check: Lock Waits
        waiting_locks = await conn.fetchval(QUERIES["lock_waits"])
        threshold = config.get_threshold("lock_waits")
        if waiting_locks > threshold.critical:
            severity = Severity.CRITICAL
        elif waiting_locks > threshold.warning:
            severity = Severity.WARNING
        else:
            severity = Severity.OK
        
        report.checks.append(CheckResult(
            name="Lock Waits",
            description="Number of queries waiting for locks",
            severity=severity,
            message=f"{waiting_locks} waiting locks",
            details={"waiting_locks": waiting_locks},
            suggestion="Investigate blocking queries" if severity != Severity.OK else None,
        ))
        
        # Check: Cache hit ratio
        cache_ratio = await conn.fetchval(QUERIES["cache_hit_ratio"])
        if cache_ratio is not None:
            threshold = config.get_threshold("cache_hit_ratio")
            ratio = float(cache_ratio)
            ratio_pct = ratio * 100
            if ratio < threshold.critical:
                severity = Severity.CRITICAL
            elif ratio < threshold.warning:
                severity = Severity.WARNING
            else:
                severity = Severity.OK
            report.checks.append(CheckResult(
                name="Cache Hit Ratio",
                description="Percentage of data reads from cache vs disk",
                severity=severity,
                message=f"Cache hit ratio: {ratio_pct:.1f}%",
                details={"ratio": ratio},
                suggestion="Increase shared_buffers if ratio is low" if severity != Severity.OK else None,
            ))
        
        # Check: Index hit ratio
        index_ratio = await conn.fetchval(QUERIES["index_hit_ratio"])
        if index_ratio is not None:
            threshold = config.get_threshold("index_hit_ratio")
            ratio = float(index_ratio)
            ratio_pct = ratio * 100
            if ratio < threshold.critical:
                severity = Severity.CRITICAL
            elif ratio < threshold.warning:
                severity = Severity.WARNING
            else:
                severity = Severity.OK
            report.checks.append(CheckResult(
                name="Index Hit Ratio",
                description="Percentage of index reads from cache",
                severity=severity,
                message=f"Index hit ratio: {ratio_pct:.1f}%",
                details={"ratio": ratio},
            ))
        
        # Check: Connection usage
        conn_info = await conn.fetchrow(QUERIES["connection_count"])
        if conn_info:
            threshold = config.get_threshold("connections")
            usage_ratio = conn_info["total"] / conn_info["max_connections"]
            usage_pct = usage_ratio * 100
            if usage_ratio > threshold.critical:
                severity = Severity.CRITICAL
            elif usage_ratio > threshold.warning:
                severity = Severity.WARNING
            else:
                severity = Severity.OK
            report.checks.append(CheckResult(
                name="Connection Usage",
                description="Current connections vs max_connections",
                severity=severity,
                message=f"{conn_info['total']}/{conn_info['max_connections']} connections ({usage_pct:.0f}%)",
                details={
                    "total": conn_info["total"],
                    "active": conn_info["active"],
                    "idle": conn_info["idle"],
                    "max": conn_info["max_connections"],
                    "usage_ratio": usage_ratio,
                },
            ))
        
        # Check: Vacuum Stats (dead tuples)
        vacuum_stats = await conn.fetch(QUERIES["vacuum_stats"])
        if vacuum_stats:
            threshold = config.get_threshold("dead_tuples")
            max_dead = max(row["n_dead_tup"] for row in vacuum_stats)
            
            if max_dead > threshold.critical:
                severity = Severity.CRITICAL
            elif max_dead > threshold.warning:
                severity = Severity.WARNING
            else:
                severity = Severity.INFO
            
            tables_with_issues = len([r for r in vacuum_stats if r["n_dead_tup"] > threshold.warning])
            
            report.checks.append(CheckResult(
                name="Vacuum Stats",
                description="Tables with high dead tuple counts",
                severity=severity,
                message=f"{tables_with_issues} tables with > {int(threshold.warning):,} dead tuples (max: {max_dead:,})",
                details={"tables_checked": len(vacuum_stats), "max_dead_tuples": max_dead},
                suggestion="Run VACUUM ANALYZE on affected tables" if severity != Severity.OK else None,
            ))
            
            # Store vacuum stats for report
            for row in vacuum_stats:
                report.vacuum_stats.append(VacuumInfo(
                    schema_name=row["schemaname"],
                    table_name=row["relname"],
                    dead_tuples=row["n_dead_tup"],
                    last_vacuum=row["last_vacuum"],
                    last_autovacuum=row["last_autovacuum"],
                ))
        else:
            report.checks.append(CheckResult(
                name="Vacuum Stats",
                description="Tables with high dead tuple counts",
                severity=Severity.OK,
                message="No tables with significant dead tuples",
            ))
        
        # Check: Long running queries
        long_queries = await conn.fetch(QUERIES["long_running_queries"])
        if long_queries:
            report.checks.append(CheckResult(
                name="Long Running Queries",
                description="Queries running for more than 5 minutes",
                severity=Severity.WARNING,
                message=f"{len(long_queries)} long-running queries detected",
                details={"queries": [dict(q) for q in long_queries]},
                suggestion="Review and optimize these queries or consider terminating",
            ))
        else:
            report.checks.append(CheckResult(
                name="Long Running Queries",
                description="Queries running for more than 5 minutes",
                severity=Severity.OK,
                message="No long-running queries",
            ))
        
        # Check: Unused indexes
        unused = await conn.fetch(QUERIES["unused_indexes"])
        stats_reset = await conn.fetchval(QUERIES["stats_reset"])
        stats_note = ""
        from datetime import datetime, timezone
        
        # If stats_reset is NULL, use postmaster start time
        if not stats_reset:
            stats_reset = await conn.fetchval("SELECT pg_postmaster_start_time();")
        
        if stats_reset:
            days_since_reset = (datetime.now(timezone.utc) - stats_reset).days
            if days_since_reset < 7:
                stats_note = f" (stats only {days_since_reset}d old - may be inaccurate)"
            else:
                stats_note = f" (since {stats_reset.strftime('%Y-%m-%d')})"
        
        if unused:
            report.checks.append(CheckResult(
                name="Unused Indexes",
                description="Indexes that have never been scanned",
                severity=Severity.WARNING if len(unused) > 5 else Severity.INFO,
                message=f"{len(unused)} unused indexes found{stats_note}",
                suggestion="Review before dropping — small tables may use seq scan instead of index scan",
            ))
            for row in unused:
                report.unused_indexes.append(IndexInfo(
                    schema_name=row["schema_name"],
                    table_name=row["table_name"],
                    index_name=row["index_name"],
                    index_size=row["index_size"],
                    index_scans=row["index_scans"],
                    is_unused=True,
                ))
        else:
            report.checks.append(CheckResult(
                name="Unused Indexes",
                description="Indexes that have never been used",
                severity=Severity.OK,
                message="No unused indexes found",
            ))
        
        # Check: Table bloat
        bloated = await conn.fetch(QUERIES["bloat_estimate"])
        threshold = config.get_threshold("table_bloat")
        high_bloat = [b for b in bloated if b["dead_ratio"] and float(b["dead_ratio"]) / 100 > threshold.warning]
        critical_bloat = [b for b in bloated if b["dead_ratio"] and float(b["dead_ratio"]) / 100 > threshold.critical]
        
        if critical_bloat:
            severity = Severity.CRITICAL
        elif high_bloat:
            severity = Severity.WARNING
        else:
            severity = Severity.OK
            
        if high_bloat:
            report.checks.append(CheckResult(
                name="Table Bloat",
                description="Tables with high dead tuple ratio",
                severity=severity,
                message=f"{len(high_bloat)} tables with >{int(threshold.warning*100)}% dead tuples",
                details={"tables": [dict(b) for b in high_bloat]},
                suggestion="Run VACUUM ANALYZE on these tables",
            ))
        else:
            report.checks.append(CheckResult(
                name="Table Bloat",
                description="Tables with high dead tuple ratio",
                severity=Severity.OK,
                message="No significant table bloat detected",
            ))
        
        # Check: Missing primary keys
        missing_pk = await conn.fetch(QUERIES["missing_primary_keys"])
        if missing_pk:
            report.checks.append(CheckResult(
                name="Missing Primary Keys",
                description="Tables without primary keys",
                severity=Severity.WARNING,
                message=f"{len(missing_pk)} tables without primary keys",
                details={"tables": [f"{r['schema_name']}.{r['table_name']}" for r in missing_pk]},
                suggestion="Add primary keys for data integrity and replication support",
            ))
        else:
            report.checks.append(CheckResult(
                name="Missing Primary Keys",
                description="Tables without primary keys",
                severity=Severity.OK,
                message="All tables have primary keys",
            ))
        
        # Get table sizes
        try:
            tables = await conn.fetch(QUERIES["table_sizes"])
            for row in tables:
                report.tables.append(TableInfo(
                    schema_name=row["schema_name"],
                    table_name=row["table_name"],
                    row_count=row["row_count"] or 0,
                    total_size=row["total_size"],
                    table_size=row["table_size"],
                    index_size=row["index_size"],
                ))
        except Exception:
            pass  # No user tables
        
        # Try to get slow queries (requires pg_stat_statements)
        try:
            slow = await conn.fetch(QUERIES["slow_queries"])
            for row in slow:
                report.slow_queries.append(SlowQuery(
                    query=row["query"][:200] + "..." if len(row["query"]) > 200 else row["query"],
                    calls=row["calls"],
                    total_time_ms=row["total_time_ms"],
                    mean_time_ms=row["mean_time_ms"],
                    rows=row["rows"],
                ))
            if slow:
                report.checks.append(CheckResult(
                    name="Slow Queries",
                    description="Queries with high average execution time",
                    severity=Severity.INFO,
                    message=f"Found {len(slow)} potentially slow queries",
                    suggestion="Review query plans and add indexes if needed",
                ))
        except asyncpg.UndefinedTableError:
            report.checks.append(CheckResult(
                name="Slow Queries",
                description="Queries with high average execution time",
                severity=Severity.INFO,
                message="pg_stat_statements extension not enabled",
                suggestion="Enable pg_stat_statements for query performance insights",
            ))
        
        return report
        
    finally:
        await conn.close()
