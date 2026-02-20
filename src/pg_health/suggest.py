"""Auto-suggest recommendations for PostgreSQL health issues."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import asyncpg

from .checks import fix_connection_string, QUERIES
from .models import HealthConfig, HealthReport, Severity


class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class Recommendation:
    """A single actionable recommendation."""
    
    priority: Priority
    title: str
    why: str
    impact: str | None = None
    sql: str | None = None
    action: str | None = None  # For non-SQL recommendations
    details: dict[str, Any] = field(default_factory=dict)
    fix_type: str | None = None  # For linking to fix commands: unused-indexes, vacuum, etc.


# Additional queries for deeper analysis
ANALYSIS_QUERIES = {
    "shared_buffers": """
        SELECT 
            current_setting('shared_buffers') as shared_buffers,
            pg_size_pretty(
                (SELECT setting::bigint * 8192 FROM pg_settings WHERE name = 'shared_buffers')
            ) as shared_buffers_size;
    """,
    
    "system_memory": """
        SELECT pg_size_pretty(
            (SELECT setting::bigint * 8192 FROM pg_settings WHERE name = 'shared_buffers')
        ) as shared_buffers_size;
    """,
    
    "unused_indexes_detailed": """
        SELECT 
            sui.schemaname as schema_name,
            sui.relname as table_name,
            sui.indexrelname as index_name,
            pg_size_pretty(pg_relation_size(sui.indexrelid)) as index_size,
            pg_relation_size(sui.indexrelid) as index_size_bytes,
            sui.idx_scan as index_scans
        FROM pg_stat_user_indexes sui
        JOIN pg_index pi ON sui.indexrelid = pi.indexrelid
        WHERE sui.idx_scan = 0
          AND NOT pi.indisprimary
          AND NOT pi.indisunique
        ORDER BY pg_relation_size(sui.indexrelid) DESC;
    """,
    
    "tables_needing_vacuum": """
        SELECT 
            schemaname,
            relname,
            n_dead_tup,
            n_live_tup,
            round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 2) as dead_pct,
            last_vacuum,
            last_autovacuum,
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) as table_size
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 10000
        ORDER BY n_dead_tup DESC;
    """,
    
    "sequential_scan_candidates": """
        SELECT 
            schemaname,
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            n_live_tup,
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) as table_size,
            pg_relation_size(schemaname || '.' || relname) as size_bytes
        FROM pg_stat_user_tables
        WHERE seq_scan > 100
          AND n_live_tup > 10000
          AND (idx_scan = 0 OR seq_scan > idx_scan * 10)
        ORDER BY seq_tup_read DESC
        LIMIT 20;
    """,
    
    "large_tables": """
        SELECT 
            schemaname,
            relname,
            pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size,
            pg_total_relation_size(schemaname || '.' || relname) as size_bytes,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        WHERE pg_total_relation_size(schemaname || '.' || relname) > 1073741824  -- > 1GB
        ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC;
    """,
    
    "missing_indexes_from_slow_queries": """
        SELECT 
            query,
            calls,
            mean_exec_time as mean_time_ms,
            rows
        FROM pg_stat_statements
        WHERE query ILIKE '%WHERE%'
          AND query NOT ILIKE '%pg_%'
          AND mean_exec_time > 100  -- > 100ms average
        ORDER BY mean_exec_time DESC
        LIMIT 10;
    """,
    
    "index_usage_stats": """
        SELECT 
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as index_size_bytes
        FROM pg_stat_user_indexes
        ORDER BY idx_scan DESC;
    """,
    
    "table_column_stats": """
        SELECT 
            schemaname,
            tablename,
            attname as column_name,
            n_distinct,
            null_frac
        FROM pg_stats
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
          AND n_distinct > 100;  -- High cardinality columns
    """,
    
    "outdated_statistics": """
        SELECT 
            schemaname,
            relname,
            last_analyze,
            last_autoanalyze,
            n_live_tup,
            n_dead_tup,
            n_mod_since_analyze
        FROM pg_stat_user_tables
        WHERE n_mod_since_analyze > n_live_tup * 0.1  -- > 10% modified since last analyze
          AND n_live_tup > 1000
        ORDER BY n_mod_since_analyze DESC
        LIMIT 20;
    """,
}


def format_size(bytes_val: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f}{unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f}PB"


async def generate_suggestions(
    connection_string: str,
    config: HealthConfig | None = None,
) -> list[Recommendation]:
    """Analyze database and generate actionable recommendations."""
    
    if config is None:
        config = HealthConfig.defaults()
    
    connection_string = fix_connection_string(connection_string)
    conn = await asyncpg.connect(connection_string)
    
    recommendations: list[Recommendation] = []
    
    try:
        # 1. Check cache hit ratio
        cache_ratio = await conn.fetchval(QUERIES["cache_hit_ratio"])
        if cache_ratio is not None:
            ratio = float(cache_ratio)
            if ratio < 0.95:  # Below 95%
                shared_buffers = await conn.fetchrow(ANALYSIS_QUERIES["shared_buffers"])
                current_size = shared_buffers["shared_buffers_size"] if shared_buffers else "unknown"
                
                priority = Priority.HIGH if ratio < 0.90 else Priority.MEDIUM
                recommendations.append(Recommendation(
                    priority=priority,
                    title="Increase shared_buffers",
                    why=f"Cache hit ratio is {ratio*100:.1f}% (should be >95%)",
                    impact="Better cache hit ratio means faster queries",
                    action=f"Edit postgresql.conf, set shared_buffers to ~25% of RAM. Current: {current_size}",
                    details={"cache_hit_ratio": ratio, "current_shared_buffers": current_size},
                ))
        
        # 2. Check unused indexes
        unused_indexes = await conn.fetch(ANALYSIS_QUERIES["unused_indexes_detailed"])
        if unused_indexes:
            total_wasted = sum(row["index_size_bytes"] for row in unused_indexes)
            for idx in unused_indexes[:5]:  # Top 5 by size
                size_bytes = idx["index_size_bytes"]
                priority = Priority.MEDIUM if size_bytes > 10_000_000 else Priority.LOW  # > 10MB
                
                schema = idx["schema_name"]
                table = idx["table_name"]
                index = idx["index_name"]
                
                recommendations.append(Recommendation(
                    priority=priority,
                    title=f"Drop unused index {index}",
                    why=f"0 scans since stats reset, {idx['index_size']} wasted",
                    impact=f"Free {idx['index_size']} disk space, faster writes",
                    sql=f"DROP INDEX {schema}.{index};",
                    details={"schema": schema, "table": table, "index": index, "size_bytes": size_bytes},
                    fix_type="unused-indexes",
                ))
            
            if len(unused_indexes) > 5:
                recommendations.append(Recommendation(
                    priority=Priority.MEDIUM,
                    title=f"Review {len(unused_indexes) - 5} more unused indexes",
                    why=f"Total {format_size(total_wasted)} wasted on unused indexes",
                    impact="Run `pg-health fix unused-indexes --dry-run` to see all",
                    fix_type="unused-indexes",
                ))
        
        # 3. Check tables needing vacuum
        vacuum_tables = await conn.fetch(ANALYSIS_QUERIES["tables_needing_vacuum"])
        for vt in vacuum_tables:
            dead_pct = float(vt["dead_pct"]) if vt["dead_pct"] else 0
            if dead_pct > 10 or vt["n_dead_tup"] > 100000:
                priority = Priority.HIGH if dead_pct > 20 or vt["n_dead_tup"] > 500000 else Priority.MEDIUM
                
                schema = vt["schemaname"]
                table = vt["relname"]
                
                recommendations.append(Recommendation(
                    priority=priority,
                    title=f"VACUUM ANALYZE {schema}.{table}",
                    why=f"{vt['n_dead_tup']:,} dead tuples ({dead_pct:.1f}% bloat)",
                    impact="Reclaim disk space, improve query performance",
                    sql=f"VACUUM ANALYZE {schema}.{table};",
                    details={"schema": schema, "table": table, "dead_tuples": vt["n_dead_tup"], "dead_pct": dead_pct},
                    fix_type="vacuum",
                ))
        
        # 4. Check for tables with heavy seq scans (might need indexes)
        seq_scan_tables = await conn.fetch(ANALYSIS_QUERIES["sequential_scan_candidates"])
        for sst in seq_scan_tables[:5]:
            if sst["n_live_tup"] > 50000 and sst["size_bytes"] > 50_000_000:  # > 50k rows and > 50MB
                schema = sst["schemaname"]
                table = sst["relname"]
                
                recommendations.append(Recommendation(
                    priority=Priority.MEDIUM,
                    title=f"Consider adding index on {schema}.{table}",
                    why=f"{sst['seq_scan']:,} sequential scans on {sst['n_live_tup']:,} rows ({sst['table_size']})",
                    impact="Index could significantly speed up queries",
                    action="Analyze query patterns to identify which columns to index",
                    details={"schema": schema, "table": table, "seq_scans": sst["seq_scan"], "rows": sst["n_live_tup"]},
                ))
        
        # 5. Check large tables for potential partitioning
        large_tables = await conn.fetch(ANALYSIS_QUERIES["large_tables"])
        for lt in large_tables:
            size_gb = lt["size_bytes"] / (1024**3)
            if size_gb > 10:  # > 10GB
                schema = lt["schemaname"]
                table = lt["relname"]
                
                recommendations.append(Recommendation(
                    priority=Priority.LOW,
                    title=f"Consider partitioning {schema}.{table}",
                    why=f"Table is {lt['total_size']} with {lt['row_count']:,} rows",
                    impact="Improved query performance, easier maintenance",
                    action="Partition by date/time column if available, or by range/list",
                    details={"schema": schema, "table": table, "size_gb": size_gb, "rows": lt["row_count"]},
                ))
        
        # 6. Check for outdated statistics
        outdated_stats = await conn.fetch(ANALYSIS_QUERIES["outdated_statistics"])
        if outdated_stats:
            tables_needing_analyze = [
                f"{row['schemaname']}.{row['relname']}" 
                for row in outdated_stats 
                if row["n_mod_since_analyze"] > 10000
            ]
            if tables_needing_analyze:
                recommendations.append(Recommendation(
                    priority=Priority.MEDIUM,
                    title="Update table statistics",
                    why=f"{len(tables_needing_analyze)} tables have outdated statistics",
                    impact="Better query plans with accurate statistics",
                    sql="ANALYZE " + ", ".join(tables_needing_analyze[:5]) + ";",
                    details={"tables": tables_needing_analyze},
                    fix_type="analyze",
                ))
        
        # 7. Try to analyze slow queries for missing indexes
        try:
            slow_queries = await conn.fetch(ANALYSIS_QUERIES["missing_indexes_from_slow_queries"])
            if slow_queries:
                for sq in slow_queries[:3]:
                    if sq["mean_time_ms"] > 500:  # > 500ms
                        recommendations.append(Recommendation(
                            priority=Priority.HIGH if sq["mean_time_ms"] > 1000 else Priority.MEDIUM,
                            title="Optimize slow query",
                            why=f"Query averaging {sq['mean_time_ms']:.0f}ms ({sq['calls']:,} calls)",
                            impact=f"~{sq['mean_time_ms']:.0f}ms saved per call",
                            action=f"Review query plan: {sq['query'][:100]}...",
                            details={"query": sq["query"][:200], "mean_time_ms": sq["mean_time_ms"], "calls": sq["calls"]},
                        ))
        except asyncpg.UndefinedTableError:
            # pg_stat_statements not enabled
            pass
        
        # 8. Check connection usage
        conn_info = await conn.fetchrow(QUERIES["connection_count"])
        if conn_info:
            usage_ratio = conn_info["total"] / conn_info["max_connections"]
            if usage_ratio > 0.7:
                recommendations.append(Recommendation(
                    priority=Priority.HIGH if usage_ratio > 0.9 else Priority.MEDIUM,
                    title="Connection pool nearing limit",
                    why=f"Using {conn_info['total']}/{conn_info['max_connections']} connections ({usage_ratio*100:.0f}%)",
                    impact="May cause connection refused errors",
                    action="Consider using connection pooler (PgBouncer) or increasing max_connections",
                    details={"current": conn_info["total"], "max": conn_info["max_connections"]},
                ))
        
        # 9. Check replication lag
        lag_seconds = await conn.fetchval(QUERIES["replication_lag"])
        if lag_seconds is not None and lag_seconds > 10:
            recommendations.append(Recommendation(
                priority=Priority.HIGH if lag_seconds > 60 else Priority.MEDIUM,
                title="High replication lag",
                why=f"Replica is {lag_seconds}s behind primary",
                impact="Stale reads, potential data loss if failover occurs",
                action="Check network latency, disk I/O, and write load on primary",
                details={"lag_seconds": lag_seconds},
            ))
        
        # 10. Check lock waits
        waiting_locks = await conn.fetchval(QUERIES["lock_waits"])
        if waiting_locks and waiting_locks > 5:
            recommendations.append(Recommendation(
                priority=Priority.HIGH if waiting_locks > 20 else Priority.MEDIUM,
                title="High lock contention",
                why=f"{waiting_locks} queries waiting for locks",
                impact="Queries blocked, potential deadlocks",
                action="Identify blocking queries with pg_blocking_pids()",
                details={"waiting_locks": waiting_locks},
            ))
    
    finally:
        await conn.close()
    
    # Sort by priority
    priority_order = {Priority.HIGH: 0, Priority.MEDIUM: 1, Priority.LOW: 2}
    recommendations.sort(key=lambda r: priority_order[r.priority])
    
    return recommendations
