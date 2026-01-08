"""
Clean up Trading Lab demo objects from Snowflake.

This script removes all objects created by the demo:
- Tasks (suspended first)
- Streams
- Tables
- Schemas
- Database
- Warehouses
- Roles

Usage:
    python cleanup.py [--confirm]

Without --confirm, shows what would be deleted (dry run).
"""

import argparse

from rich.console import Console
from rich.prompt import Confirm
from snowflake.snowpark import Session


def cleanup_tasks(session: Session, dry_run: bool, console: Console) -> None:
    """Suspend and drop all tasks."""
    tasks = [
        ("STAGE", "TASK_PUBLISH_CURATED"),
        ("STAGE", "TASK_TRANSFORM_TRADES"),
        ("STAGE", "TASK_UPDATE_POSITIONS"),
    ]

    for schema, task in tasks:
        fqn = f"TRADING_LAB_DB.{schema}.{task}"
        if dry_run:
            console.print(f"  [dim]Would drop task:[/dim] {fqn}")
        else:
            try:
                # Suspend first
                session.sql(f"ALTER TASK IF EXISTS {fqn} SUSPEND").collect()
                session.sql(f"DROP TASK IF EXISTS {fqn}").collect()
                console.print(f"  [green]Dropped task:[/green] {fqn}")
            except Exception as e:
                console.print(f"  [red]Error dropping task {fqn}:[/red] {e}")


def cleanup_streams(session: Session, dry_run: bool, console: Console) -> None:
    """Drop all streams."""
    streams = [
        ("RAW", "TRADES_RAW_STREAM"),
        ("RAW", "MARKET_EVENTS_RAW_STREAM"),
        ("RAW", "POSITIONS_RAW_STREAM"),
    ]

    for schema, stream in streams:
        fqn = f"TRADING_LAB_DB.{schema}.{stream}"
        if dry_run:
            console.print(f"  [dim]Would drop stream:[/dim] {fqn}")
        else:
            try:
                session.sql(f"DROP STREAM IF EXISTS {fqn}").collect()
                console.print(f"  [green]Dropped stream:[/green] {fqn}")
            except Exception as e:
                console.print(f"  [red]Error dropping stream {fqn}:[/red] {e}")


def cleanup_database(session: Session, dry_run: bool, console: Console) -> None:
    """Drop the database (cascades to all schemas/tables)."""
    if dry_run:
        console.print("  [dim]Would drop database:[/dim] TRADING_LAB_DB")
    else:
        try:
            session.sql("DROP DATABASE IF EXISTS TRADING_LAB_DB CASCADE").collect()
            console.print("  [green]Dropped database:[/green] TRADING_LAB_DB")
        except Exception as e:
            console.print(f"  [red]Error dropping database:[/red] {e}")


def cleanup_warehouses(session: Session, dry_run: bool, console: Console) -> None:
    """Drop all demo warehouses."""
    warehouses = [
        "TRADING_INGEST_WH",
        "TRADING_TRANSFORM_WH",
        "TRADING_ANALYTICS_WH",
    ]

    for wh in warehouses:
        if dry_run:
            console.print(f"  [dim]Would drop warehouse:[/dim] {wh}")
        else:
            try:
                session.sql(f"DROP WAREHOUSE IF EXISTS {wh}").collect()
                console.print(f"  [green]Dropped warehouse:[/green] {wh}")
            except Exception as e:
                console.print(f"  [red]Error dropping warehouse {wh}:[/red] {e}")


def cleanup_roles(session: Session, dry_run: bool, console: Console) -> None:
    """Drop demo roles."""
    roles = ["TRADING_LAB_ROLE", "DBT_TRADING_ROLE"]

    for role in roles:
        if dry_run:
            console.print(f"  [dim]Would drop role:[/dim] {role}")
        else:
            try:
                session.sql(f"DROP ROLE IF EXISTS {role}").collect()
                console.print(f"  [green]Dropped role:[/green] {role}")
            except Exception as e:
                console.print(f"  [red]Error dropping role {role}:[/red] {e}")


def cleanup_integrations(session: Session, dry_run: bool, console: Console) -> None:
    """Drop storage and Git integrations."""
    integrations = [
        ("STORAGE INTEGRATION", "TRADING_S3_INT"),
        ("API INTEGRATION", "TRADING_GIT_INT"),
    ]

    for int_type, name in integrations:
        if dry_run:
            console.print(f"  [dim]Would drop {int_type}:[/dim] {name}")
        else:
            try:
                session.sql(f"DROP {int_type} IF EXISTS {name}").collect()
                console.print(f"  [green]Dropped {int_type}:[/green] {name}")
            except Exception as e:
                console.print(f"  [yellow]Skipped {int_type} {name}:[/yellow] {e}")


def main() -> int:
    """
    Run cleanup operations.

    Returns:
        Exit code (0 = success)
    """
    parser = argparse.ArgumentParser(
        description="Clean up Trading Lab demo objects from Snowflake"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually delete objects (without this flag, shows dry run)",
    )
    args = parser.parse_args()

    console = Console()
    dry_run = not args.confirm

    console.print("\n[bold blue]Trading Lab Cleanup[/bold blue]\n")

    if dry_run:
        console.print(
            "[yellow]DRY RUN MODE[/yellow] - No changes will be made.\n"
            "Use --confirm to actually delete objects.\n"
        )

    # Create session
    try:
        session = Session.builder.config("connection_name", "default").create()
    except Exception as e:
        console.print(f"[red]Failed to connect to Snowflake:[/red] {e}")
        return 1

    # Confirm if not dry run
    if not dry_run:
        if not Confirm.ask(
            "[bold red]This will DELETE all Trading Lab objects. Continue?[/bold red]"
        ):
            console.print("Cancelled.")
            session.close()
            return 0

    try:
        console.print("[bold]Cleaning up tasks...[/bold]")
        cleanup_tasks(session, dry_run, console)

        console.print("\n[bold]Cleaning up streams...[/bold]")
        cleanup_streams(session, dry_run, console)

        console.print("\n[bold]Cleaning up database...[/bold]")
        cleanup_database(session, dry_run, console)

        console.print("\n[bold]Cleaning up warehouses...[/bold]")
        cleanup_warehouses(session, dry_run, console)

        console.print("\n[bold]Cleaning up roles...[/bold]")
        cleanup_roles(session, dry_run, console)

        console.print("\n[bold]Cleaning up integrations...[/bold]")
        cleanup_integrations(session, dry_run, console)

    finally:
        session.close()

    console.print()
    if dry_run:
        console.print(
            "[yellow]Dry run complete.[/yellow] "
            "Run with --confirm to delete objects."
        )
    else:
        console.print("[green]Cleanup complete![/green]")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

