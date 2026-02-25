"""CLI for PG Health."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .checks import run_health_check
from .models import Severity, HealthConfig, ThresholdConfig
from .suggest import generate_suggestions, Priority, Recommendation
from .fix import (
    fix_unused_indexes,
    fix_vacuum,
    fix_analyze,
    fix_all,
    FixType,
    FixResult,
)

load_dotenv()

app = typer.Typer(
    name="pg-health",
    help="PostgreSQL health check and optimization tool.",
    no_args_is_help=True,
)
console = Console()

SEVERITY_COLORS = {
    Severity.OK: "green",
    Severity.INFO: "blue",
    Severity.WARNING: "yellow",
    Severity.CRITICAL: "red",
}

SEVERITY_ICONS = {
    Severity.OK: "✅",
    Severity.INFO: "ℹ️",
    Severity.WARNING: "⚠️",
    Severity.CRITICAL: "❌",
}

EXIT_CODES = {
    Severity.OK: 0,
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.CRITICAL: 2,
}


def load_config(config_path: Path | None) -> HealthConfig:
    """Load configuration from YAML file or env var."""
    # Check env var first
    if config_path is None:
        env_config = os.getenv("PG_HEALTH_CONFIG")
        if env_config:
            config_path = Path(env_config)
    
    if config_path is None or not config_path.exists():
        return HealthConfig.defaults()
    
    try:
        import yaml
        with open(config_path) as f:
            data = yaml.safe_load(f)
        
        if not data or "thresholds" not in data:
            return HealthConfig.defaults()
        
        thresholds = {}
        for name, values in data["thresholds"].items():
            thresholds[name] = ThresholdConfig(
                warning=values.get("warning", 0.8),
                critical=values.get("critical", 0.9),
            )
        
        return HealthConfig(thresholds=thresholds)
    except ImportError:
        console.print("[yellow]Warning: PyYAML not installed, using default thresholds[/yellow]")
        return HealthConfig.defaults()
    except Exception as e:
        console.print(f"[yellow]Warning: Could not load config: {e}[/yellow]")
        return HealthConfig.defaults()


@app.command()
def check(
    connection: Annotated[
        str | None,
        typer.Option("--connection", "-c", help="PostgreSQL connection string"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output JSON file"),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", "-j", help="Output results as JSON to stdout"),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Output only status: OK, WARNING, or CRITICAL"),
    ] = False,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Path to YAML config file for thresholds"),
    ] = None,
    save: Annotated[
        bool,
        typer.Option("--save", help="Save results to history database for trending"),
    ] = False,
):
    """Run health checks on a PostgreSQL database.
    
    Exit codes: 0=OK, 1=WARNING, 2=CRITICAL
    
    Use --save to record results for historical trending.
    """
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        if json_output:
            print(json.dumps({"ok": False, "error": "No connection string provided"}))
        elif quiet:
            print("CRITICAL")
        else:
            console.print("[red]Error: No connection string provided.[/red]")
            console.print("Use --connection or set DATABASE_URL in .env")
        raise typer.Exit(2)
    
    # Load config
    health_config = load_config(config)
    
    if not quiet and not json_output:
        console.print("[bold]Running PostgreSQL health checks...[/bold]\n")
    
    try:
        report = asyncio.run(run_health_check(conn_str, health_config))
    except Exception as e:
        if json_output:
            print(json.dumps({"ok": False, "error": str(e)}))
        elif quiet:
            print("CRITICAL")
        else:
            console.print(f"[red]Connection failed: {e}[/red]")
        raise typer.Exit(2)
    
    # Determine exit code based on worst severity
    worst = report.worst_severity
    exit_code = EXIT_CODES.get(worst, 0)
    
    # Save to history if requested
    if save:
        from .history import save_report
        import hashlib
        conn_hash = hashlib.md5(conn_str.encode()).hexdigest()[:8]
        save_report(report, conn_hash)
        if not quiet and not json_output:
            console.print("[dim]📊 Saved to history[/dim]\n")
    
    # Quiet mode - just output status
    if quiet:
        print(worst.value.upper())
        raise typer.Exit(exit_code)
    
    # JSON output mode
    if json_output:
        result = {
            "ok": worst in (Severity.OK, Severity.INFO),
            "status": worst.value,
            "report": report.model_dump(mode="json"),
        }
        print(json.dumps(result, indent=2, default=str))
        raise typer.Exit(exit_code)
    
    # Full console output
    # Display header
    console.print(Panel(
        f"[bold]{report.database_name}[/bold]\n{report.database_version}",
        title="Database",
    ))
    
    # Display check results
    table = Table(title="Health Checks")
    table.add_column("Status", width=3)
    table.add_column("Check")
    table.add_column("Result")
    table.add_column("Suggestion")
    
    for chk in report.checks:
        icon = SEVERITY_ICONS[chk.severity]
        color = SEVERITY_COLORS[chk.severity]
        table.add_row(
            icon,
            chk.name,
            f"[{color}]{chk.message}[/{color}]",
            chk.suggestion or "-",
        )
    
    console.print(table)
    
    # Summary
    summary = report.summary
    console.print(f"\n[bold]Summary:[/bold] "
                  f"[green]{summary[Severity.OK]} OK[/green], "
                  f"[blue]{summary[Severity.INFO]} Info[/blue], "
                  f"[yellow]{summary[Severity.WARNING]} Warnings[/yellow], "
                  f"[red]{summary[Severity.CRITICAL]} Critical[/red]")
    
    # Show vacuum stats if any
    if report.vacuum_stats:
        console.print(f"\n[bold yellow]Tables with High Dead Tuples:[/bold yellow]")
        for v in report.vacuum_stats[:5]:
            vacuum_info = ""
            if v.last_autovacuum:
                vacuum_info = f" (last autovacuum: {v.last_autovacuum.strftime('%Y-%m-%d %H:%M')})"
            elif v.last_vacuum:
                vacuum_info = f" (last vacuum: {v.last_vacuum.strftime('%Y-%m-%d %H:%M')})"
            console.print(f"  • {v.schema_name}.{v.table_name}: {v.dead_tuples:,} dead tuples{vacuum_info}")
    
    # Show unused indexes if any
    if report.unused_indexes:
        console.print(f"\n[bold yellow]Unused Indexes ({len(report.unused_indexes)}):[/bold yellow]")
        for idx in report.unused_indexes[:5]:
            console.print(f"  • {idx.table_name}.{idx.index_name} ({idx.index_size})")
        if len(report.unused_indexes) > 5:
            console.print(f"  ... and {len(report.unused_indexes) - 5} more")
    
    # Show largest tables
    if report.tables:
        console.print(f"\n[bold]Largest Tables:[/bold]")
        for t in report.tables[:5]:
            console.print(f"  • {t.schema_name}.{t.table_name}: {t.total_size} ({t.row_count:,} rows)")
    
    # Show slow queries if any
    if report.slow_queries:
        console.print(f"\n[bold]Slowest Queries:[/bold]")
        for sq in report.slow_queries[:3]:
            console.print(f"  • {sq.mean_time_ms:.0f}ms avg ({sq.calls} calls)")
            console.print(f"    [dim]{sq.query[:80]}...[/dim]")
    
    # Save JSON if requested
    if output:
        with open(output, "w") as f:
            json.dump(report.model_dump(mode="json"), f, indent=2, default=str)
        console.print(f"\n[green]Report saved to {output}[/green]")
    
    raise typer.Exit(exit_code)


@app.command()
def badge(
    connection: Annotated[
        str | None,
        typer.Option("--connection", "-c", help="PostgreSQL connection string"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output SVG file (defaults to stdout)"),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Path to YAML config file for thresholds"),
    ] = None,
):
    """Generate an SVG status badge showing database health.
    
    Colors: green (OK), yellow (WARNING), red (CRITICAL)
    """
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        console.print("[red]Error: No connection string provided.[/red]")
        raise typer.Exit(1)
    
    # Load config
    health_config = load_config(config)
    
    try:
        report = asyncio.run(run_health_check(conn_str, health_config))
    except Exception as e:
        # Generate error badge
        svg = generate_badge("error", "red")
        if output:
            output.write_text(svg)
        else:
            print(svg)
        raise typer.Exit(2)
    
    # Determine status
    worst = report.worst_severity
    summary = report.summary
    
    # Determine badge text and color
    if worst == Severity.CRITICAL:
        critical_count = summary[Severity.CRITICAL]
        text = f"{critical_count} critical" if critical_count > 1 else "CRITICAL"
        color = "#e05d44"  # red
    elif worst == Severity.WARNING:
        warning_count = summary[Severity.WARNING]
        text = f"{warning_count} warnings" if warning_count > 1 else "WARNING"
        color = "#dfb317"  # yellow
    else:
        text = "OK"
        color = "#4c1"  # green
    
    svg = generate_badge(text, color)
    
    if output:
        output.write_text(svg)
        console.print(f"[green]Badge saved to {output}[/green]")
    else:
        print(svg)


def generate_badge(text: str, color: str) -> str:
    """Generate an SVG badge with the given text and color."""
    
    label = "DB Health"
    label_width = len(label) * 6 + 10
    text_width = len(text) * 6 + 10
    total_width = label_width + text_width
    
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20">
  <linearGradient id="b" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="a">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </clipPath>
  <g clip-path="url(#a)">
    <rect width="{label_width}" height="20" fill="#555"/>
    <rect x="{label_width}" width="{text_width}" height="20" fill="{color}"/>
    <rect width="{total_width}" height="20" fill="url(#b)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11">
    <text x="{label_width/2}" y="15" fill="#010101" fill-opacity=".3">{label}</text>
    <text x="{label_width/2}" y="14" fill="#fff">{label}</text>
    <text x="{label_width + text_width/2}" y="15" fill="#010101" fill-opacity=".3">{text}</text>
    <text x="{label_width + text_width/2}" y="14" fill="#fff">{text}</text>
  </g>
</svg>'''
    
    return svg


@app.command()
def serve(
    host: Annotated[str, typer.Option("--host", "-h")] = "0.0.0.0",
    port: Annotated[int, typer.Option("--port", "-p")] = 8767,
):
    """Start the web interface."""
    import uvicorn
    from .web import app as web_app
    
    console.print(f"[bold]Starting PG Health web interface...[/bold]")
    console.print(f"Open http://localhost:{port} in your browser")
    uvicorn.run(web_app, host=host, port=port)


PRIORITY_COLORS = {
    Priority.HIGH: "red",
    Priority.MEDIUM: "yellow",
    Priority.LOW: "green",
}

PRIORITY_ICONS = {
    Priority.HIGH: "🔴",
    Priority.MEDIUM: "🟡",
    Priority.LOW: "🟢",
}


@app.command()
def suggest(
    connection: Annotated[
        str | None,
        typer.Option("--connection", "-c", help="PostgreSQL connection string"),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", "-j", help="Output results as JSON"),
    ] = False,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Path to YAML config file"),
    ] = None,
):
    """Analyze database and provide actionable recommendations.
    
    Examines your database for potential issues and suggests specific
    fixes with SQL commands you can run.
    """
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        if json_output:
            print(json.dumps({"ok": False, "error": "No connection string provided"}))
        else:
            console.print("[red]Error: No connection string provided.[/red]")
            console.print("Use --connection or set DATABASE_URL in .env")
        raise typer.Exit(2)
    
    # Load config
    health_config = load_config(config)
    
    if not json_output:
        console.print("[bold]🔍 Analyzing database health...[/bold]\n")
    
    try:
        recommendations = asyncio.run(generate_suggestions(conn_str, health_config))
    except Exception as e:
        if json_output:
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            console.print(f"[red]Connection failed: {e}[/red]")
        raise typer.Exit(2)
    
    # JSON output mode
    if json_output:
        result = {
            "ok": True,
            "recommendations": [
                {
                    "priority": r.priority.value,
                    "title": r.title,
                    "why": r.why,
                    "impact": r.impact,
                    "sql": r.sql,
                    "action": r.action,
                    "fix_type": r.fix_type,
                    "details": r.details,
                }
                for r in recommendations
            ],
        }
        print(json.dumps(result, indent=2, default=str))
        raise typer.Exit(0)
    
    if not recommendations:
        console.print("[bold green]✨ No recommendations - your database looks healthy![/bold green]")
        raise typer.Exit(0)
    
    # Group by priority
    console.print("━" * 60)
    console.print("[bold]Recommendations[/bold]")
    console.print("━" * 60 + "\n")
    
    current_priority = None
    counter = 0
    
    for rec in recommendations:
        # Print priority header if changed
        if rec.priority != current_priority:
            current_priority = rec.priority
            color = PRIORITY_COLORS[rec.priority]
            icon = PRIORITY_ICONS[rec.priority]
            console.print(f"\n{icon} [bold {color}]{rec.priority.value.upper()} PRIORITY[/bold {color}]\n")
        
        counter += 1
        console.print(f"[bold]{counter}. {rec.title}[/bold]")
        console.print(f"   [dim]Why:[/dim] {rec.why}")
        
        if rec.impact:
            console.print(f"   [dim]Impact:[/dim] {rec.impact}")
        
        if rec.sql:
            console.print(f"   [dim]SQL:[/dim] [cyan]{rec.sql}[/cyan]")
        elif rec.action:
            console.print(f"   [dim]Action:[/dim] {rec.action}")
        
        console.print()
    
    # Show fix command hints
    console.print("━" * 60)
    console.print("\n[bold]Quick Fix Commands:[/bold]")
    console.print("  pg-health fix unused-indexes -c \"...\" --dry-run")
    console.print("  pg-health fix vacuum -c \"...\" --dry-run")
    console.print("  pg-health fix all -c \"...\" --dry-run")
    console.print("\n[dim]Add --dry-run to preview changes before executing.[/dim]")


@app.command()
def fix(
    issue: Annotated[
        str,
        typer.Argument(help="Issue to fix: unused-indexes, vacuum, analyze, all"),
    ],
    connection: Annotated[
        str | None,
        typer.Option("--connection", "-c", help="PostgreSQL connection string"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", "-n", help="Preview changes without executing"),
    ] = False,
    tables: Annotated[
        str | None,
        typer.Option("--tables", "-t", help="Comma-separated list of tables (for vacuum/analyze)"),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", "-j", help="Output results as JSON"),
    ] = False,
):
    """Apply quick fixes to common database issues.
    
    Safe fixes (can auto-execute):
    - unused-indexes: DROP unused indexes
    - vacuum: VACUUM ANALYZE tables with high dead tuples
    - analyze: Update table statistics
    - all: Run all safe fixes
    
    Use --dry-run to preview what would be executed.
    """
    
    # Validate issue type
    try:
        fix_type = FixType(issue)
    except ValueError:
        valid = ", ".join(ft.value for ft in FixType)
        if json_output:
            print(json.dumps({"ok": False, "error": f"Invalid issue type. Valid: {valid}"}))
        else:
            console.print(f"[red]Invalid issue type: {issue}[/red]")
            console.print(f"Valid options: {valid}")
        raise typer.Exit(1)
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        if json_output:
            print(json.dumps({"ok": False, "error": "No connection string provided"}))
        else:
            console.print("[red]Error: No connection string provided.[/red]")
            console.print("Use --connection or set DATABASE_URL in .env")
        raise typer.Exit(2)
    
    # Parse tables list
    table_list = None
    if tables:
        table_list = [t.strip() for t in tables.split(",")]
    
    if not json_output:
        mode = "[yellow]DRY RUN[/yellow]" if dry_run else "[red]EXECUTING[/red]"
        console.print(f"[bold]🔧 Fix: {issue}[/bold] ({mode})\n")
    
    try:
        if fix_type == FixType.UNUSED_INDEXES:
            results = asyncio.run(fix_unused_indexes(conn_str, dry_run))
        elif fix_type == FixType.VACUUM:
            results = asyncio.run(fix_vacuum(conn_str, dry_run, table_list))
        elif fix_type == FixType.ANALYZE:
            results = asyncio.run(fix_analyze(conn_str, dry_run, table_list))
        elif fix_type == FixType.ALL:
            results = asyncio.run(fix_all(conn_str, dry_run))
        else:
            results = []
    except Exception as e:
        if json_output:
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(2)
    
    # JSON output
    if json_output:
        result = {
            "ok": all(r.success for r in results),
            "dry_run": dry_run,
            "results": [
                {
                    "fix_type": r.fix_type,
                    "sql": r.sql,
                    "executed": r.executed,
                    "success": r.success,
                    "message": r.message,
                    "details": r.details,
                }
                for r in results
            ],
        }
        print(json.dumps(result, indent=2, default=str))
        raise typer.Exit(0 if result["ok"] else 1)
    
    if not results:
        console.print("[bold green]✨ Nothing to fix![/bold green]")
        raise typer.Exit(0)
    
    # Display results
    for r in results:
        if r.success:
            icon = "📋" if not r.executed else "✅"
            console.print(f"{icon} {r.message}")
            console.print(f"   [dim]{r.sql}[/dim]")
        else:
            console.print(f"❌ {r.message}")
        console.print()
    
    # Summary
    total = len(results)
    executed = sum(1 for r in results if r.executed)
    success = sum(1 for r in results if r.success)
    
    console.print("━" * 60)
    if dry_run:
        console.print(f"[bold]Summary:[/bold] {total} operations would be executed")
        console.print("\n[yellow]Run without --dry-run to apply these changes.[/yellow]")
    else:
        console.print(f"[bold]Summary:[/bold] {success}/{executed} operations successful")
    
    raise typer.Exit(0 if all(r.success for r in results) else 1)


@app.command()
def notify(
    connection: Annotated[
        str | None,
        typer.Option("--connection", "-c", help="PostgreSQL connection string"),
    ] = None,
    provider: Annotated[
        str,
        typer.Option("--provider", "-p", help="Notification provider: telegram, slack, webhook"),
    ] = "telegram",
    only_issues: Annotated[
        bool,
        typer.Option("--only-issues/--always", help="Only notify if there are issues"),
    ] = True,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Path to YAML config file"),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output as JSON"),
    ] = False,
):
    """
    Run health check and send notification.
    
    Supports: telegram, slack, webhook
    
    Environment variables:
      - PG_HEALTH_TELEGRAM_TOKEN: Telegram bot token
      - PG_HEALTH_TELEGRAM_CHAT_ID: Telegram chat ID
      - PG_HEALTH_SLACK_WEBHOOK: Slack incoming webhook URL
      - PG_HEALTH_WEBHOOK_URL: Generic webhook URL
    """
    from .notify import send_telegram, send_slack, send_webhook, send_email, NotifyResult
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        if json_output:
            print(json.dumps({"ok": False, "error": "No connection string provided"}))
        else:
            console.print("[red]Error: No connection string provided.[/red]")
        raise typer.Exit(2)
    
    # Run health check
    health_config = load_config(config)
    try:
        report = asyncio.run(run_health_check(conn_str, health_config))
    except Exception as e:
        if json_output:
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            console.print(f"[red]Error running health check: {e}[/red]")
        raise typer.Exit(2)
    
    # Send notification
    providers = {
        "telegram": send_telegram,
        "slack": send_slack,
        "webhook": send_webhook,
        "email": send_email,
    }
    
    if provider not in providers:
        if json_output:
            print(json.dumps({"ok": False, "error": f"Unknown provider: {provider}"}))
        else:
            console.print(f"[red]Unknown provider: {provider}[/red]")
            console.print(f"Valid: {', '.join(providers.keys())}")
        raise typer.Exit(1)
    
    result = providers[provider](report, only_on_issues=only_issues)
    
    if json_output:
        print(json.dumps({
            "ok": result.success,
            "provider": result.provider,
            "message": result.message,
            "error": result.error,
            "health_status": report.worst_severity.value,
        }))
    else:
        if result.success:
            console.print(f"[green]✅ {result.provider}: {result.message}[/green]")
        else:
            console.print(f"[red]❌ {result.provider}: {result.error}[/red]")
    
    raise typer.Exit(0 if result.success else 1)


@app.command()
def history(
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Filter by database name"),
    ] = None,
    days: Annotated[
        int,
        typer.Option("--days", help="Look back this many days"),
    ] = 7,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Maximum entries to show"),
    ] = 20,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output as JSON"),
    ] = False,
):
    """
    View health check history.
    
    Requires running checks with --save to populate history.
    """
    from .history import get_history, get_databases
    
    entries = get_history(database, days, limit)
    
    if json_output:
        data = [
            {
                "id": e.id,
                "database": e.database_name,
                "checked_at": e.checked_at.isoformat(),
                "status": e.worst_severity,
                "warnings": e.warnings,
                "criticals": e.criticals,
            }
            for e in entries
        ]
        print(json.dumps(data, indent=2))
        raise typer.Exit(0)
    
    if not entries:
        console.print("[yellow]No history found.[/yellow]")
        console.print("Run health checks with --save to record history.")
        raise typer.Exit(0)
    
    # Display table
    table = Table(title=f"Health Check History (last {days} days)")
    table.add_column("Time", style="dim")
    table.add_column("Database")
    table.add_column("Status")
    table.add_column("Warnings", justify="right")
    table.add_column("Critical", justify="right")
    
    for e in entries:
        status_color = {
            "ok": "green",
            "info": "blue", 
            "warning": "yellow",
            "critical": "red",
        }.get(e.worst_severity, "white")
        
        table.add_row(
            e.checked_at.strftime("%Y-%m-%d %H:%M"),
            e.database_name,
            f"[{status_color}]{e.worst_severity.upper()}[/{status_color}]",
            str(e.warnings),
            str(e.criticals) if e.criticals else "-",
        )
    
    console.print(table)
    
    # Show available databases
    dbs = get_databases()
    if len(dbs) > 1:
        console.print(f"\n[dim]Databases with history: {', '.join(dbs)}[/dim]")


@app.command()
def trend(
    database: Annotated[
        str,
        typer.Argument(help="Database name"),
    ],
    metric: Annotated[
        str | None,
        typer.Option("--metric", "-m", help="Metric name (e.g., 'Cache Hit Ratio.ratio')"),
    ] = None,
    days: Annotated[
        int,
        typer.Option("--days", help="Look back this many days"),
    ] = 7,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output as JSON"),
    ] = False,
):
    """
    View metric trends over time.
    
    Use --metric to specify which metric to show.
    Without --metric, lists available metrics.
    """
    from .history import get_metric_trend, get_available_metrics
    
    if not metric:
        # List available metrics
        metrics = get_available_metrics(database)
        if json_output:
            print(json.dumps({"database": database, "metrics": metrics}))
        else:
            console.print(f"[bold]Available metrics for {database}:[/bold]")
            for m in metrics:
                console.print(f"  • {m}")
            if not metrics:
                console.print("[yellow]No metrics found. Run checks with --save first.[/yellow]")
        raise typer.Exit(0)
    
    points = get_metric_trend(database, metric, days)
    
    if json_output:
        data = [
            {"timestamp": p.timestamp.isoformat(), "value": p.value}
            for p in points
        ]
        print(json.dumps({"database": database, "metric": metric, "points": data}, indent=2))
        raise typer.Exit(0)
    
    if not points:
        console.print(f"[yellow]No data for metric '{metric}'[/yellow]")
        raise typer.Exit(0)
    
    console.print(f"[bold]{metric}[/bold] (last {days} days)\n")
    
    # Simple ASCII chart
    values = [p.value for p in points]
    min_val, max_val = min(values), max(values)
    
    if min_val == max_val:
        console.print(f"Constant value: {min_val}")
    else:
        # Normalize to 0-20 range for display
        chart_height = 10
        for i, p in enumerate(points[-20:]):  # Last 20 points
            normalized = int((p.value - min_val) / (max_val - min_val) * chart_height)
            bar = "█" * normalized + "░" * (chart_height - normalized)
            console.print(f"{p.timestamp.strftime('%m-%d %H:%M')} {bar} {p.value:.2f}")
    
    console.print(f"\n[dim]Min: {min_val:.2f}, Max: {max_val:.2f}, Latest: {values[-1]:.2f}[/dim]")


if __name__ == "__main__":
    app()
