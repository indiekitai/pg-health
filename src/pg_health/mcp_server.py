#!/usr/bin/env python3
"""
pg-health MCP Server

Provides PostgreSQL health check tools for AI agents:
- pg_health_check: Run comprehensive health check
- pg_health_suggest: Get optimization recommendations
- pg_health_fix: Apply fixes (with dry-run support)
"""

import asyncio
import json
import sys
from typing import Optional

# Import from main module
from .checks import run_health_check
from .suggest import generate_suggestions, Priority
from .fix import (
    fix_unused_indexes,
    fix_vacuum,
    fix_analyze,
    fix_all,
    FixType,
)
from .models import Severity

# FastMCP is optional
try:
    from fastmcp import FastMCP
    mcp = FastMCP("pg-health")
    HAS_MCP = True
except ImportError:
    HAS_MCP = False
    
    class DummyMCP:
        def tool(self):
            def decorator(f):
                return f
            return decorator
    mcp = DummyMCP()


def severity_to_emoji(severity: Severity) -> str:
    """Convert severity to emoji for readability."""
    return {
        Severity.OK: "✅",
        Severity.INFO: "ℹ️",
        Severity.WARNING: "⚠️",
        Severity.CRITICAL: "❌",
    }.get(severity, "❓")


@mcp.tool()
def pg_health_check(connection_string: str) -> str:
    """
    Run a comprehensive PostgreSQL health check.
    
    Returns a JSON report with:
    - overall_status: ok, warning, or critical
    - checks: Array of individual check results
    - database_info: Version, size, uptime
    - tables: Top tables by size
    - unused_indexes: Indexes that are never used
    
    Each check includes:
    - name: Check identifier
    - severity: ok, info, warning, critical
    - message: Human-readable result
    - suggestion: What to do if there's an issue
    
    Args:
        connection_string: PostgreSQL connection URL 
                          (e.g., postgresql://user:pass@host:5432/dbname)
    """
    try:
        report = asyncio.run(run_health_check(connection_string))
        
        # Build a more digestible summary
        checks_summary = []
        for check in report.checks:
            checks_summary.append({
                "name": check.name,
                "status": f"{severity_to_emoji(check.severity)} {check.severity.value}",
                "message": check.message,
                "suggestion": check.suggestion,
            })
        
        # Count by severity
        severity_counts = {}
        for check in report.checks:
            sev = check.severity.value
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
        
        result = {
            "overall_status": report.worst_severity.value,
            "has_issues": report.has_issues,
            "summary": severity_counts,
            "database": {
                "name": report.database_name,
                "version": report.database_version,
            },
            "checks": checks_summary,
            "top_tables": [
                {
                    "name": f"{t.schema_name}.{t.table_name}",
                    "rows": t.row_count,
                    "size": t.total_size,
                }
                for t in (report.tables or [])[:5]
            ],
            "unused_indexes_count": len(report.unused_indexes or []),
            "unused_indexes": [
                {
                    "name": f"{i.schema_name}.{i.index_name}",
                    "table": i.table_name,
                    "size": i.index_size,
                }
                for i in (report.unused_indexes or [])[:5]
            ],
        }
        
        return json.dumps(result, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "error": str(e),
            "hint": "Check connection string format: postgresql://user:pass@host:5432/dbname"
        })


@mcp.tool()
def pg_health_suggest(connection_string: str) -> str:
    """
    Get actionable optimization recommendations for a PostgreSQL database.
    
    Analyzes the database and returns prioritized recommendations:
    - HIGH priority: Issues that need immediate attention
    - MEDIUM priority: Optimizations that would help performance
    - LOW priority: Nice-to-have improvements
    
    Each recommendation includes:
    - title: What to do
    - why: Why it matters
    - impact: Expected improvement
    - sql: SQL command to fix (if applicable)
    - fix_type: Can be used with pg_health_fix()
    
    Args:
        connection_string: PostgreSQL connection URL
    """
    try:
        suggestions = asyncio.run(generate_suggestions(connection_string))
        
        if not suggestions:
            return json.dumps({
                "status": "healthy",
                "message": "No issues found! Database looks good.",
                "recommendations": []
            })
        
        # Group by priority
        by_priority = {"high": [], "medium": [], "low": []}
        for s in suggestions:
            rec = {
                "title": s.title,
                "why": s.why,
                "impact": s.impact,
                "fix_type": s.fix_type,
            }
            if s.sql:
                rec["sql"] = s.sql
            if s.action:
                rec["action"] = s.action
            by_priority[s.priority.value].append(rec)
        
        result = {
            "status": "needs_attention" if by_priority["high"] else "could_improve",
            "total_recommendations": len(suggestions),
            "high_priority": by_priority["high"],
            "medium_priority": by_priority["medium"],
            "low_priority": by_priority["low"],
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def pg_health_fix(
    connection_string: str,
    fix_type: str = "all",
    dry_run: bool = True,
    tables: Optional[str] = None,
) -> str:
    """
    Apply fixes to PostgreSQL database issues.
    
    ⚠️ IMPORTANT: By default runs in dry_run mode (shows SQL but doesn't execute).
    Set dry_run=False to actually execute fixes.
    
    Available fix types:
    - unused-indexes: Drop indexes that are never used
    - vacuum: Run VACUUM on tables with dead tuples
    - analyze: Update table statistics
    - all: Run all safe fixes
    
    Args:
        connection_string: PostgreSQL connection URL
        fix_type: Type of fix to apply (unused-indexes, vacuum, analyze, all)
        dry_run: If True, show SQL without executing. Default: True
        tables: Comma-separated list of tables (for vacuum/analyze)
    """
    try:
        table_list = tables.split(",") if tables else None
        
        # Map string to enum
        fix_map = {
            "unused-indexes": FixType.UNUSED_INDEXES,
            "vacuum": FixType.VACUUM,
            "analyze": FixType.ANALYZE,
            "all": FixType.ALL,
        }
        
        if fix_type not in fix_map:
            return json.dumps({
                "error": f"Unknown fix type: {fix_type}",
                "available": list(fix_map.keys())
            })
        
        fix_enum = fix_map[fix_type]
        
        # Run the appropriate fix
        if fix_enum == FixType.UNUSED_INDEXES:
            results = asyncio.run(fix_unused_indexes(connection_string, dry_run=dry_run))
        elif fix_enum == FixType.VACUUM:
            results = asyncio.run(fix_vacuum(connection_string, tables=table_list, dry_run=dry_run))
        elif fix_enum == FixType.ANALYZE:
            results = asyncio.run(fix_analyze(connection_string, tables=table_list, dry_run=dry_run))
        else:  # all
            results = asyncio.run(fix_all(connection_string, dry_run=dry_run))
        
        # Format results
        formatted = []
        for r in results:
            formatted.append({
                "fix_type": r.fix_type,
                "sql": r.sql,
                "executed": r.executed,
                "success": r.success,
                "message": r.message,
            })
        
        return json.dumps({
            "dry_run": dry_run,
            "fix_type": fix_type,
            "results": formatted,
            "note": "Set dry_run=False to actually execute these fixes" if dry_run else "Fixes applied"
        }, indent=2)
        
    except Exception as e:
        return json.dumps({"error": str(e)})


def main():
    """Run the MCP server."""
    if not HAS_MCP:
        print("Error: fastmcp not installed.", file=sys.stderr)
        print("Install with: pip install fastmcp", file=sys.stderr)
        print("\nYou can still use the tool functions directly:", file=sys.stderr)
        print("  from pg_health.mcp_server import pg_health_check, pg_health_suggest", file=sys.stderr)
        sys.exit(1)
    mcp.run()


if __name__ == "__main__":
    main()
