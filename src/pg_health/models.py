"""Data models for PG Health."""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class Severity(str, Enum):
    OK = "ok"
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class CheckResult(BaseModel):
    """Result of a single health check."""
    
    name: str
    description: str
    severity: Severity
    message: str
    details: dict = Field(default_factory=dict)
    suggestion: str | None = None


class TableInfo(BaseModel):
    """Information about a table."""
    
    schema_name: str
    table_name: str
    row_count: int
    total_size: str
    table_size: str
    index_size: str
    

class IndexInfo(BaseModel):
    """Information about an index."""
    
    schema_name: str
    table_name: str
    index_name: str
    index_size: str
    index_scans: int
    is_unused: bool


class SlowQuery(BaseModel):
    """A slow query from pg_stat_statements."""
    
    query: str
    calls: int
    total_time_ms: float
    mean_time_ms: float
    rows: int


class HealthReport(BaseModel):
    """Complete health check report."""
    
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    database_name: str
    database_version: str
    checks: list[CheckResult] = Field(default_factory=list)
    tables: list[TableInfo] = Field(default_factory=list)
    unused_indexes: list[IndexInfo] = Field(default_factory=list)
    slow_queries: list[SlowQuery] = Field(default_factory=list)
    
    @property
    def summary(self) -> dict[Severity, int]:
        """Count of checks by severity."""
        counts = {s: 0 for s in Severity}
        for check in self.checks:
            counts[check.severity] += 1
        return counts
    
    @property
    def has_issues(self) -> bool:
        """Whether there are any warnings or critical issues."""
        return any(c.severity in (Severity.WARNING, Severity.CRITICAL) for c in self.checks)
