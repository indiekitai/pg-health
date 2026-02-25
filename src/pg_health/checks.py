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
    
    # New checks - added based on competitor analysis
    "duplicate_indexes_v2": """
        SELECT 
            pg_size_pretty(sum(pg_relation_size(idx))::bigint) as total_size,
            (array_agg(idx::text))[1] as index1,
            (array_agg(idx::text))[2] as index2,
            tbl::text as table_name
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
    
    "fk_missing_indexes": """
        SELECT 
            c.conname as constraint_name,
            c.conrelid::regclass as table_name,
            a.attname as column_name,
            c.confrelid::regclass as referenced_table
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
        WHERE c.contype = 'f'
          AND NOT EXISTS (
              SELECT 1 FROM pg_index i 
              WHERE i.indrelid = c.conrelid 
                AND a.attnum = ANY(i.indkey)
          );
    """,
    
    "wal_stats": """
        SELECT 
            (SELECT count(*) FROM pg_ls_waldir()) as wal_files,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as wal_total_size
        WHERE pg_is_in_recovery() = false;
    """,
    
    "tablespace_usage": """
        SELECT 
            spcname as name,
            pg_size_pretty(pg_tablespace_size(oid)) as size,
            pg_tablespace_location(oid) as location
        FROM pg_tablespace;
    """,
    
    "replication_slots": """
        SELECT 
            slot_name,
            slot_type,
            database,
            active,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained_wal,
            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as retained_bytes
        FROM pg_replication_slots
        WHERE NOT temporary;
    """,
    
    "bgwriter_stats": """
        SELECT 
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset
        FROM pg_stat_bgwriter;
    """,
    
    "wal_stats": """
        SELECT 
            (SELECT count(*) FROM pg_ls_waldir()) as wal_files,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'max_wal_size') as max_wal_size
        WHERE NOT pg_is_in_recovery();
    """,
    
    "config_audit": """
        SELECT 
            name,
            setting,
            unit,
            context,
            CASE 
                WHEN name = 'shared_buffers' AND setting::bigint * 
                    CASE WHEN unit = '8kB' THEN 8192 WHEN unit = 'kB' THEN 1024 ELSE 1 END < 268435456 
                THEN 'Consider increasing shared_buffers (currently ' || 
                    pg_size_pretty(setting::bigint * CASE WHEN unit = '8kB' THEN 8192 WHEN unit = 'kB' THEN 1024 ELSE 1 END) || 
                    ', recommend 25% of RAM, min 256MB)'
                WHEN name = 'work_mem' AND setting::bigint < 4096 
                THEN 'Consider increasing work_mem (currently ' || setting || 'kB, recommend 4-64MB for complex queries)'
                WHEN name = 'maintenance_work_mem' AND setting::bigint < 65536 
                THEN 'Consider increasing maintenance_work_mem for faster VACUUM/CREATE INDEX'
                WHEN name = 'effective_cache_size' AND setting::bigint * 8192 < 536870912 
                THEN 'Consider increasing effective_cache_size (recommend 50-75% of RAM)'
                WHEN name = 'random_page_cost' AND setting::numeric > 1.5 AND EXISTS (SELECT 1 FROM pg_settings WHERE name = 'data_directory')
                THEN 'Consider lowering random_page_cost to 1.1-1.5 for SSD storage'
                WHEN name = 'checkpoint_completion_target' AND setting::numeric < 0.9 
                THEN 'Consider setting checkpoint_completion_target to 0.9'
                WHEN name = 'wal_buffers' AND setting::bigint < 2048 
                THEN 'Consider increasing wal_buffers (recommend 64MB for write-heavy workloads)'
                ELSE NULL
            END as recommendation
        FROM pg_settings 
        WHERE name IN (
            'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size',
            'max_connections', 'checkpoint_completion_target', 'random_page_cost',
            'wal_buffers', 'max_wal_size', 'min_wal_size', 'wal_level'
        )
        ORDER BY name;
    """,
    
    "config_recommendations": """
        SELECT 
            name,
            setting,
            unit,
            boot_val,
            source,
            CASE 
                WHEN name = 'shared_buffers' THEN 
                    CASE WHEN setting::bigint * 8192 < 134217728 THEN 'Consider increasing (currently ' || pg_size_pretty(setting::bigint * 8192) || ', recommend 25% of RAM)'
                    ELSE 'OK' END
                WHEN name = 'work_mem' THEN
                    CASE WHEN setting::bigint < 4096 THEN 'Consider increasing (currently ' || setting || 'kB, recommend 4-64MB)'
                    ELSE 'OK' END
                WHEN name = 'maintenance_work_mem' THEN
                    CASE WHEN setting::bigint < 65536 THEN 'Consider increasing for faster VACUUM/CREATE INDEX'
                    ELSE 'OK' END
                WHEN name = 'effective_cache_size' THEN
                    CASE WHEN setting::bigint * 8192 < 536870912 THEN 'Consider increasing (recommend 50-75% of RAM)'
                    ELSE 'OK' END
                ELSE 'OK'
            END as recommendation
        FROM pg_settings 
        WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size', 
                       'max_connections', 'checkpoint_completion_target', 'random_page_cost');
    """,
    
    "security_checks": """
        SELECT 
            'public_schema_permissions' as check_name,
            CASE 
                WHEN has_schema_privilege('public', 'public', 'CREATE') 
                THEN 'WARNING: public role can create objects in public schema'
                ELSE 'OK'
            END as status
        UNION ALL
        SELECT 
            'superuser_count',
            'INFO: ' || count(*) || ' superuser roles' 
        FROM pg_roles WHERE rolsuper = true;
    """,
    
    "table_age": """
        SELECT 
            t.schemaname || '.' || t.relname as table_name,
            age(c.relfrozenxid) as xid_age,
            CASE 
                WHEN age(c.relfrozenxid) > 1000000000 THEN 'CRITICAL: approaching wraparound'
                WHEN age(c.relfrozenxid) > 500000000 THEN 'WARNING: needs vacuum freeze soon'
                ELSE 'OK'
            END as status
        FROM pg_stat_user_tables t
        JOIN pg_class c ON c.relname = t.relname 
          AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.schemaname)
        WHERE age(c.relfrozenxid) > 100000000
        ORDER BY age(c.relfrozenxid) DESC
        LIMIT 10;
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
        
        # NEW: Check for duplicate indexes
        try:
            duplicates = await conn.fetch(QUERIES["duplicate_indexes_v2"])
            if duplicates:
                total_wasted = sum(1 for d in duplicates)  # count pairs
                report.checks.append(CheckResult(
                    name="Duplicate Indexes",
                    description="Indexes with identical columns on same table",
                    severity=Severity.WARNING,
                    message=f"{total_wasted} duplicate index pair(s) found",
                    details={"duplicates": [dict(d) for d in duplicates]},
                    suggestion="Review and drop redundant indexes to save space",
                ))
            else:
                report.checks.append(CheckResult(
                    name="Duplicate Indexes",
                    description="Indexes with identical columns on same table",
                    severity=Severity.OK,
                    message="No duplicate indexes found",
                ))
        except Exception:
            pass
        
        # NEW: Check for foreign keys missing indexes
        try:
            fk_no_idx = await conn.fetch(QUERIES["fk_missing_indexes"])
            if fk_no_idx:
                report.checks.append(CheckResult(
                    name="FK Missing Indexes",
                    description="Foreign key columns without indexes",
                    severity=Severity.WARNING if len(fk_no_idx) > 3 else Severity.INFO,
                    message=f"{len(fk_no_idx)} foreign keys without indexes",
                    details={"missing": [dict(f) for f in fk_no_idx]},
                    suggestion="Add indexes on FK columns for faster JOINs and CASCADE deletes",
                ))
            else:
                report.checks.append(CheckResult(
                    name="FK Missing Indexes",
                    description="Foreign key columns without indexes",
                    severity=Severity.OK,
                    message="All foreign keys have indexes",
                ))
        except Exception:
            pass
        
        # NEW: Check for table age (transaction ID wraparound)
        try:
            aged_tables = await conn.fetch(QUERIES["table_age"])
            critical_age = [t for t in aged_tables if 'CRITICAL' in t['status']]
            warning_age = [t for t in aged_tables if 'WARNING' in t['status']]
            
            if critical_age:
                severity = Severity.CRITICAL
            elif warning_age:
                severity = Severity.WARNING
            else:
                severity = Severity.OK
            
            if aged_tables:
                max_age = max(t['xid_age'] for t in aged_tables)
                report.checks.append(CheckResult(
                    name="Transaction ID Age",
                    description="Table age approaching wraparound threshold",
                    severity=severity,
                    message=f"Max XID age: {max_age:,} ({len(critical_age)} critical, {len(warning_age)} warning)",
                    details={"tables": [dict(t) for t in aged_tables[:5]]},
                    suggestion="Run VACUUM FREEZE on old tables" if severity != Severity.OK else None,
                ))
            else:
                report.checks.append(CheckResult(
                    name="Transaction ID Age",
                    description="Table age approaching wraparound threshold",
                    severity=Severity.OK,
                    message="All tables have healthy XID age",
                ))
        except Exception:
            pass
        
        # NEW: Security checks
        try:
            security = await conn.fetch(QUERIES["security_checks"])
            warnings = [s for s in security if 'WARNING' in s['status']]
            
            if warnings:
                report.checks.append(CheckResult(
                    name="Security Checks",
                    description="Basic security configuration audit",
                    severity=Severity.WARNING,
                    message=f"{len(warnings)} security warning(s)",
                    details={"checks": [dict(s) for s in security]},
                    suggestion="Review and fix security warnings",
                ))
            else:
                report.checks.append(CheckResult(
                    name="Security Checks",
                    description="Basic security configuration audit",
                    severity=Severity.OK,
                    message="No security issues detected",
                    details={"checks": [dict(s) for s in security]},
                ))
        except Exception:
            pass
        
        # NEW: Tablespace usage
        try:
            tablespaces = await conn.fetch(QUERIES["tablespace_usage"])
            if tablespaces:
                report.checks.append(CheckResult(
                    name="Tablespace Usage",
                    description="Tablespace sizes and locations",
                    severity=Severity.INFO,
                    message=f"{len(tablespaces)} tablespace(s)",
                    details={"tablespaces": [dict(t) for t in tablespaces]},
                ))
        except Exception:
            pass
        
        # NEW: Replication slots
        try:
            slots = await conn.fetch(QUERIES["replication_slots"])
            if slots:
                inactive_slots = [s for s in slots if not s['active']]
                # Check for slots retaining too much WAL (> 1GB)
                large_retention = [s for s in slots if s['retained_bytes'] and s['retained_bytes'] > 1073741824]
                
                if large_retention:
                    severity = Severity.WARNING
                    msg = f"{len(slots)} slot(s), {len(large_retention)} retaining >1GB WAL"
                elif inactive_slots:
                    severity = Severity.INFO
                    msg = f"{len(slots)} slot(s), {len(inactive_slots)} inactive"
                else:
                    severity = Severity.OK
                    msg = f"{len(slots)} replication slot(s), all healthy"
                
                report.checks.append(CheckResult(
                    name="Replication Slots",
                    description="Replication slot status and WAL retention",
                    severity=severity,
                    message=msg,
                    details={"slots": [dict(s) for s in slots]},
                    suggestion="Drop unused slots to free WAL space" if large_retention or inactive_slots else None,
                ))
            else:
                report.checks.append(CheckResult(
                    name="Replication Slots",
                    description="Replication slot status",
                    severity=Severity.INFO,
                    message="No replication slots configured",
                ))
        except Exception:
            pass
        
        # NEW: BG Writer stats
        try:
            bgw = await conn.fetchrow(QUERIES["bgwriter_stats"])
            if bgw:
                total_checkpoints = (bgw['checkpoints_timed'] or 0) + (bgw['checkpoints_req'] or 0)
                requested_pct = (bgw['checkpoints_req'] / total_checkpoints * 100) if total_checkpoints > 0 else 0
                
                # High requested checkpoint ratio indicates checkpoint_timeout too high or max_wal_size too low
                if requested_pct > 50 and total_checkpoints > 10:
                    severity = Severity.WARNING
                    suggestion = "High requested checkpoint ratio - consider increasing max_wal_size"
                else:
                    severity = Severity.OK
                    suggestion = None
                
                report.checks.append(CheckResult(
                    name="Background Writer",
                    description="Checkpoint and buffer write statistics",
                    severity=severity,
                    message=f"{total_checkpoints} checkpoints ({requested_pct:.0f}% requested)",
                    details={
                        "checkpoints_timed": bgw['checkpoints_timed'],
                        "checkpoints_requested": bgw['checkpoints_req'],
                        "buffers_checkpoint": bgw['buffers_checkpoint'],
                        "buffers_backend": bgw['buffers_backend'],
                        "buffers_backend_fsync": bgw['buffers_backend_fsync'],
                    },
                    suggestion=suggestion,
                ))
        except Exception:
            pass
        
        # NEW: WAL stats (primary only)
        try:
            wal = await conn.fetchrow(QUERIES["wal_stats"])
            if wal and wal['wal_files']:
                report.checks.append(CheckResult(
                    name="WAL Statistics",
                    description="Write-ahead log file count and settings",
                    severity=Severity.INFO,
                    message=f"{wal['wal_files']} WAL files, level={wal['wal_level']}, archive={wal['archive_mode']}",
                    details={
                        "wal_files": wal['wal_files'],
                        "wal_level": wal['wal_level'],
                        "archive_mode": wal['archive_mode'],
                        "max_wal_size": wal['max_wal_size'],
                    },
                ))
        except Exception:
            pass
        
        # NEW: Configuration audit
        try:
            configs = await conn.fetch(QUERIES["config_audit"])
            recommendations = [c for c in configs if c['recommendation']]
            
            if recommendations:
                report.checks.append(CheckResult(
                    name="Configuration Audit",
                    description="PostgreSQL configuration vs best practices",
                    severity=Severity.INFO if len(recommendations) <= 2 else Severity.WARNING,
                    message=f"{len(recommendations)} configuration suggestion(s)",
                    details={"configs": [dict(c) for c in configs], "recommendations": [dict(r) for r in recommendations]},
                    suggestion=recommendations[0]['recommendation'] if recommendations else None,
                ))
            else:
                report.checks.append(CheckResult(
                    name="Configuration Audit",
                    description="PostgreSQL configuration vs best practices",
                    severity=Severity.OK,
                    message="Configuration looks good",
                    details={"configs": [dict(c) for c in configs]},
                ))
        except Exception:
            pass
        
        return report
        
    finally:
        await conn.close()
