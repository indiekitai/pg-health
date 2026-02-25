"""Historical data storage for pg-health trends."""

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from .models import HealthReport, Severity


def get_db_path() -> Path:
    """Get path to history database."""
    data_dir = Path(os.getenv("PG_HEALTH_DATA_DIR", Path.home() / ".pg-health"))
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "history.db"


def init_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Initialize database and create tables if needed."""
    path = db_path or get_db_path()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS health_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            worst_severity TEXT NOT NULL,
            has_issues BOOLEAN NOT NULL,
            total_checks INTEGER NOT NULL,
            warnings INTEGER NOT NULL,
            criticals INTEGER NOT NULL,
            checks_json TEXT,
            connection_hash TEXT
        )
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_health_checks_db_time 
        ON health_checks(database_name, checked_at)
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            connection_hash TEXT
        )
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_db_metric_time 
        ON metrics(database_name, metric_name, checked_at)
    """)
    
    conn.commit()
    return conn


def save_report(report: HealthReport, connection_hash: Optional[str] = None) -> int:
    """
    Save health report to history database.
    
    Returns: row ID of saved report
    """
    conn = init_db()
    
    warnings = len([c for c in report.checks if c.severity == Severity.WARNING])
    criticals = len([c for c in report.checks if c.severity == Severity.CRITICAL])
    
    checks_data = [
        {
            "name": c.name,
            "severity": c.severity.value,
            "message": c.message,
        }
        for c in report.checks
    ]
    
    cursor = conn.execute("""
        INSERT INTO health_checks 
        (database_name, worst_severity, has_issues, total_checks, warnings, criticals, checks_json, connection_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        report.database_name,
        report.worst_severity.value,
        report.has_issues,
        len(report.checks),
        warnings,
        criticals,
        json.dumps(checks_data),
        connection_hash,
    ))
    
    row_id = cursor.lastrowid
    
    # Save individual metrics for trending
    for check in report.checks:
        if check.details:
            # Extract numeric metrics
            for key, value in check.details.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    conn.execute("""
                        INSERT INTO metrics (database_name, metric_name, metric_value, connection_hash)
                        VALUES (?, ?, ?, ?)
                    """, (report.database_name, f"{check.name}.{key}", value, connection_hash))
    
    conn.commit()
    conn.close()
    
    return row_id


@dataclass
class HistoryEntry:
    """A single health check history entry."""
    id: int
    database_name: str
    checked_at: datetime
    worst_severity: str
    has_issues: bool
    total_checks: int
    warnings: int
    criticals: int


def get_history(
    database_name: Optional[str] = None,
    days: int = 7,
    limit: int = 100,
) -> List[HistoryEntry]:
    """
    Get health check history.
    
    Args:
        database_name: Filter by database (optional)
        days: Look back this many days
        limit: Maximum entries to return
    """
    conn = init_db()
    
    since = datetime.now() - timedelta(days=days)
    
    if database_name:
        rows = conn.execute("""
            SELECT * FROM health_checks 
            WHERE database_name = ? AND checked_at >= ?
            ORDER BY checked_at DESC
            LIMIT ?
        """, (database_name, since, limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM health_checks 
            WHERE checked_at >= ?
            ORDER BY checked_at DESC
            LIMIT ?
        """, (since, limit)).fetchall()
    
    conn.close()
    
    return [
        HistoryEntry(
            id=row["id"],
            database_name=row["database_name"],
            checked_at=datetime.fromisoformat(row["checked_at"]),
            worst_severity=row["worst_severity"],
            has_issues=bool(row["has_issues"]),
            total_checks=row["total_checks"],
            warnings=row["warnings"],
            criticals=row["criticals"],
        )
        for row in rows
    ]


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: datetime
    value: float


def get_metric_trend(
    database_name: str,
    metric_name: str,
    days: int = 7,
) -> List[MetricPoint]:
    """
    Get historical trend for a specific metric.
    
    Example metric names:
    - "Cache Hit Ratio.ratio"
    - "Connection Usage.usage_ratio"
    - "Lock Waits.waiting_locks"
    """
    conn = init_db()
    
    since = datetime.now() - timedelta(days=days)
    
    rows = conn.execute("""
        SELECT checked_at, metric_value FROM metrics 
        WHERE database_name = ? AND metric_name = ? AND checked_at >= ?
        ORDER BY checked_at
    """, (database_name, metric_name, since)).fetchall()
    
    conn.close()
    
    return [
        MetricPoint(
            timestamp=datetime.fromisoformat(row["checked_at"]),
            value=row["metric_value"],
        )
        for row in rows
    ]


def get_databases() -> List[str]:
    """Get list of databases with history."""
    conn = init_db()
    rows = conn.execute("""
        SELECT DISTINCT database_name FROM health_checks ORDER BY database_name
    """).fetchall()
    conn.close()
    return [row["database_name"] for row in rows]


def get_available_metrics(database_name: str) -> List[str]:
    """Get list of available metrics for a database."""
    conn = init_db()
    rows = conn.execute("""
        SELECT DISTINCT metric_name FROM metrics 
        WHERE database_name = ?
        ORDER BY metric_name
    """, (database_name,)).fetchall()
    conn.close()
    return [row["metric_name"] for row in rows]


def cleanup_old_data(days: int = 90):
    """Delete data older than specified days."""
    conn = init_db()
    cutoff = datetime.now() - timedelta(days=days)
    
    conn.execute("DELETE FROM health_checks WHERE checked_at < ?", (cutoff,))
    conn.execute("DELETE FROM metrics WHERE checked_at < ?", (cutoff,))
    
    conn.commit()
    conn.close()
