"""Data models for PG Health."""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class Severity(str, Enum):
    OK = "ok"
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ThresholdConfig(BaseModel):
    """Threshold configuration for a single check."""
    warning: float
    critical: float


class HealthConfig(BaseModel):
    """Configuration for health check thresholds."""
    
    thresholds: dict[str, ThresholdConfig] = Field(default_factory=dict)
    
    @classmethod
    def defaults(cls) -> "HealthConfig":
        """Return default thresholds."""
        return cls(thresholds={
            "cache_hit_ratio": ThresholdConfig(warning=0.95, critical=0.90),
            "index_hit_ratio": ThresholdConfig(warning=0.95, critical=0.90),
            "connections": ThresholdConfig(warning=0.70, critical=0.90),
            "replication_lag": ThresholdConfig(warning=10, critical=60),
            "dead_tuples": ThresholdConfig(warning=100000, critical=1000000),
            "lock_waits": ThresholdConfig(warning=5, critical=20),
            "table_bloat": ThresholdConfig(warning=0.10, critical=0.20),  # 10%, 20%
        })
    
    def get_threshold(self, name: str) -> ThresholdConfig:
        """Get threshold for a check, using defaults if not configured."""
        defaults = self.defaults().thresholds
        if name in self.thresholds:
            return self.thresholds[name]
        return defaults.get(name, ThresholdConfig(warning=0.8, critical=0.9))


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


class VacuumInfo(BaseModel):
    """Vacuum status for a table."""
    
    schema_name: str
    table_name: str
    dead_tuples: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None


class HealthReport(BaseModel):
    """Complete health check report."""
    
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    database_name: str
    database_version: str
    checks: list[CheckResult] = Field(default_factory=list)
    tables: list[TableInfo] = Field(default_factory=list)
    unused_indexes: list[IndexInfo] = Field(default_factory=list)
    slow_queries: list[SlowQuery] = Field(default_factory=list)
    vacuum_stats: list[VacuumInfo] = Field(default_factory=list)
    
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
    
    @property
    def worst_severity(self) -> Severity:
        """Return the worst severity level from all checks."""
        if any(c.severity == Severity.CRITICAL for c in self.checks):
            return Severity.CRITICAL
        if any(c.severity == Severity.WARNING for c in self.checks):
            return Severity.WARNING
        return Severity.OK
