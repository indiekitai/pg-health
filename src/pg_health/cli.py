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
):
    """Run health checks on a PostgreSQL database.
    
    Exit codes: 0=OK, 1=WARNING, 2=CRITICAL
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


if __name__ == "__main__":
    app()
