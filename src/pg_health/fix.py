"""Quick fix functionality for PostgreSQL health issues."""

from dataclasses import dataclass
from enum import Enum

import asyncpg

from .checks import fix_connection_string
from .suggest import ANALYSIS_QUERIES


class FixType(str, Enum):
    """Types of fixes that can be applied."""
    UNUSED_INDEXES = "unused-indexes"
    VACUUM = "vacuum"
    ANALYZE = "analyze"
    ALL = "all"


class FixSafety(str, Enum):
    """Safety level of a fix."""
    SAFE = "safe"  # Can be auto-executed
    UNSAFE = "unsafe"  # Only show SQL, don't execute


@dataclass
class FixResult:
    """Result of a fix operation."""
    
    fix_type: str
    sql: str
    executed: bool
    success: bool
    message: str
    details: dict | None = None


# Which fixes are safe to auto-execute
FIX_SAFETY = {
    FixType.UNUSED_INDEXES: FixSafety.SAFE,
    FixType.VACUUM: FixSafety.SAFE,
    FixType.ANALYZE: FixSafety.SAFE,
}


async def get_unused_indexes(conn: asyncpg.Connection) -> list[dict]:
    """Get list of unused indexes."""
    rows = await conn.fetch(ANALYSIS_QUERIES["unused_indexes_detailed"])
    return [
        {
            "schema": row["schema_name"],
            "table": row["table_name"],
            "index": row["index_name"],
            "size": row["index_size"],
            "size_bytes": row["index_size_bytes"],
        }
        for row in rows
    ]


async def get_tables_needing_vacuum(
    conn: asyncpg.Connection,
    tables: list[str] | None = None,
) -> list[dict]:
    """Get list of tables needing vacuum."""
    rows = await conn.fetch(ANALYSIS_QUERIES["tables_needing_vacuum"])
    result = []
    for row in rows:
        schema = row["schemaname"]
        table = row["relname"]
        full_name = f"{schema}.{table}"
        
        # Filter by specified tables if provided
        if tables and table not in tables and full_name not in tables:
            continue
        
        result.append({
            "schema": schema,
            "table": table,
            "dead_tuples": row["n_dead_tup"],
            "dead_pct": float(row["dead_pct"]) if row["dead_pct"] else 0,
            "table_size": row["table_size"],
        })
    
    return result


async def get_tables_needing_analyze(conn: asyncpg.Connection) -> list[dict]:
    """Get tables with outdated statistics."""
    query = """
        SELECT 
            schemaname,
            relname,
            n_mod_since_analyze,
            n_live_tup
        FROM pg_stat_user_tables
        WHERE n_mod_since_analyze > GREATEST(n_live_tup * 0.1, 1000)
        ORDER BY n_mod_since_analyze DESC;
    """
    rows = await conn.fetch(query)
    return [
        {
            "schema": row["schemaname"],
            "table": row["relname"],
            "modifications": row["n_mod_since_analyze"],
            "rows": row["n_live_tup"],
        }
        for row in rows
    ]


async def fix_unused_indexes(
    connection_string: str,
    dry_run: bool = True,
    limit: int | None = None,
) -> list[FixResult]:
    """Drop unused indexes."""
    
    connection_string = fix_connection_string(connection_string)
    conn = await asyncpg.connect(connection_string)
    
    results = []
    
    try:
        unused = await get_unused_indexes(conn)
        
        if limit:
            unused = unused[:limit]
        
        for idx in unused:
            schema = idx["schema"]
            index = idx["index"]
            sql = f'DROP INDEX "{schema}"."{index}";'
            
            if dry_run:
                results.append(FixResult(
                    fix_type=FixType.UNUSED_INDEXES.value,
                    sql=sql,
                    executed=False,
                    success=True,
                    message=f"Would drop index {schema}.{index} ({idx['size']})",
                    details=idx,
                ))
            else:
                try:
                    await conn.execute(sql)
                    results.append(FixResult(
                        fix_type=FixType.UNUSED_INDEXES.value,
                        sql=sql,
                        executed=True,
                        success=True,
                        message=f"Dropped index {schema}.{index} ({idx['size']})",
                        details=idx,
                    ))
                except Exception as e:
                    results.append(FixResult(
                        fix_type=FixType.UNUSED_INDEXES.value,
                        sql=sql,
                        executed=True,
                        success=False,
                        message=f"Failed to drop {schema}.{index}: {e}",
                        details=idx,
                    ))
    finally:
        await conn.close()
    
    return results


async def fix_vacuum(
    connection_string: str,
    dry_run: bool = True,
    tables: list[str] | None = None,
    analyze: bool = True,
) -> list[FixResult]:
    """Run VACUUM (ANALYZE) on tables with high dead tuple counts."""
    
    connection_string = fix_connection_string(connection_string)
    conn = await asyncpg.connect(connection_string)
    
    results = []
    
    try:
        tables_to_vacuum = await get_tables_needing_vacuum(conn, tables)
        
        for tbl in tables_to_vacuum:
            schema = tbl["schema"]
            table = tbl["table"]
            
            if analyze:
                sql = f'VACUUM ANALYZE "{schema}"."{table}";'
            else:
                sql = f'VACUUM "{schema}"."{table}";'
            
            if dry_run:
                results.append(FixResult(
                    fix_type=FixType.VACUUM.value,
                    sql=sql,
                    executed=False,
                    success=True,
                    message=f"Would vacuum {schema}.{table} ({tbl['dead_tuples']:,} dead tuples, {tbl['dead_pct']:.1f}% bloat)",
                    details=tbl,
                ))
            else:
                try:
                    # VACUUM cannot run inside a transaction
                    await conn.execute(sql)
                    results.append(FixResult(
                        fix_type=FixType.VACUUM.value,
                        sql=sql,
                        executed=True,
                        success=True,
                        message=f"Vacuumed {schema}.{table}",
                        details=tbl,
                    ))
                except Exception as e:
                    results.append(FixResult(
                        fix_type=FixType.VACUUM.value,
                        sql=sql,
                        executed=True,
                        success=False,
                        message=f"Failed to vacuum {schema}.{table}: {e}",
                        details=tbl,
                    ))
    finally:
        await conn.close()
    
    return results


async def fix_analyze(
    connection_string: str,
    dry_run: bool = True,
    tables: list[str] | None = None,
) -> list[FixResult]:
    """Run ANALYZE to update table statistics."""
    
    connection_string = fix_connection_string(connection_string)
    conn = await asyncpg.connect(connection_string)
    
    results = []
    
    try:
        tables_to_analyze = await get_tables_needing_analyze(conn)
        
        # Filter by specified tables if provided
        if tables:
            tables_to_analyze = [
                t for t in tables_to_analyze
                if t["table"] in tables or f"{t['schema']}.{t['table']}" in tables
            ]
        
        for tbl in tables_to_analyze:
            schema = tbl["schema"]
            table = tbl["table"]
            sql = f'ANALYZE "{schema}"."{table}";'
            
            if dry_run:
                results.append(FixResult(
                    fix_type=FixType.ANALYZE.value,
                    sql=sql,
                    executed=False,
                    success=True,
                    message=f"Would analyze {schema}.{table} ({tbl['modifications']:,} modifications since last analyze)",
                    details=tbl,
                ))
            else:
                try:
                    await conn.execute(sql)
                    results.append(FixResult(
                        fix_type=FixType.ANALYZE.value,
                        sql=sql,
                        executed=True,
                        success=True,
                        message=f"Analyzed {schema}.{table}",
                        details=tbl,
                    ))
                except Exception as e:
                    results.append(FixResult(
                        fix_type=FixType.ANALYZE.value,
                        sql=sql,
                        executed=True,
                        success=False,
                        message=f"Failed to analyze {schema}.{table}: {e}",
                        details=tbl,
                    ))
    finally:
        await conn.close()
    
    return results


async def fix_all(
    connection_string: str,
    dry_run: bool = True,
) -> list[FixResult]:
    """Run all safe fixes."""
    
    results = []
    
    # Unused indexes
    results.extend(await fix_unused_indexes(connection_string, dry_run))
    
    # Vacuum
    results.extend(await fix_vacuum(connection_string, dry_run))
    
    # Analyze (tables not covered by vacuum)
    results.extend(await fix_analyze(connection_string, dry_run))
    
    return results


# Map fix types to functions
FIX_FUNCTIONS = {
    FixType.UNUSED_INDEXES: fix_unused_indexes,
    FixType.VACUUM: fix_vacuum,
    FixType.ANALYZE: fix_analyze,
    FixType.ALL: fix_all,
}
