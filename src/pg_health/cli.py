"""CLI for PG Health."""

import asyncio
import json
import os
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .checks import run_health_check
from .models import Severity

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
):
    """Run health checks on a PostgreSQL database."""
    
    conn_str = connection or os.getenv("DATABASE_URL")
    if not conn_str:
        console.print("[red]Error: No connection string provided.[/red]")
        console.print("Use --connection or set DATABASE_URL in .env")
        raise typer.Exit(1)
    
    console.print("[bold]Running PostgreSQL health checks...[/bold]\n")
    
    try:
        report = asyncio.run(run_health_check(conn_str))
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        raise typer.Exit(1)
    
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
    
    for check in report.checks:
        icon = SEVERITY_ICONS[check.severity]
        color = SEVERITY_COLORS[check.severity]
        table.add_row(
            icon,
            check.name,
            f"[{color}]{check.message}[/{color}]",
            check.suggestion or "-",
        )
    
    console.print(table)
    
    # Summary
    summary = report.summary
    console.print(f"\n[bold]Summary:[/bold] "
                  f"[green]{summary[Severity.OK]} OK[/green], "
                  f"[blue]{summary[Severity.INFO]} Info[/blue], "
                  f"[yellow]{summary[Severity.WARNING]} Warnings[/yellow], "
                  f"[red]{summary[Severity.CRITICAL]} Critical[/red]")
    
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
