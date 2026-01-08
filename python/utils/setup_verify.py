"""
Verify Snowflake Trading Lab demo setup is complete.

Checks for:
- Database and schemas exist
- Warehouses exist and are accessible
- Tables are created
- Streams are created
- Tasks are created and running

Usage:
    python setup_verify.py
"""

from dataclasses import dataclass

from rich.console import Console
from rich.table import Table
from snowflake.snowpark import Session


@dataclass
class CheckResult:
    """Result of a verification check."""

    name: str
    passed: bool
    details: str = ""


def check_database(session: Session) -> CheckResult:
    """Verify database exists."""
    try:
        result = session.sql("SHOW DATABASES LIKE 'TRADING_LAB_DB'").collect()
        if len(result) > 0:
            return CheckResult("Database TRADING_LAB_DB", True, "Exists")
        return CheckResult("Database TRADING_LAB_DB", False, "Not found")
    except Exception as e:
        return CheckResult("Database TRADING_LAB_DB", False, str(e))


def check_schemas(session: Session) -> list[CheckResult]:
    """Verify all required schemas exist."""
    results = []
    required_schemas = ["RAW", "STAGE", "CURATED", "ANALYTICS"]

    try:
        existing = session.sql(
            "SHOW SCHEMAS IN DATABASE TRADING_LAB_DB"
        ).collect()
        existing_names = {row["name"] for row in existing}

        for schema in required_schemas:
            if schema in existing_names:
                results.append(CheckResult(f"Schema {schema}", True, "Exists"))
            else:
                results.append(CheckResult(f"Schema {schema}", False, "Not found"))
    except Exception as e:
        for schema in required_schemas:
            results.append(CheckResult(f"Schema {schema}", False, str(e)))

    return results


def check_warehouses(session: Session) -> list[CheckResult]:
    """Verify all required warehouses exist."""
    results = []
    required_wh = ["TRADING_INGEST_WH", "TRADING_TRANSFORM_WH", "TRADING_ANALYTICS_WH"]

    try:
        existing = session.sql("SHOW WAREHOUSES LIKE 'TRADING%'").collect()
        existing_names = {row["name"] for row in existing}

        for wh in required_wh:
            if wh in existing_names:
                results.append(CheckResult(f"Warehouse {wh}", True, "Exists"))
            else:
                results.append(CheckResult(f"Warehouse {wh}", False, "Not found"))
    except Exception as e:
        for wh in required_wh:
            results.append(CheckResult(f"Warehouse {wh}", False, str(e)))

    return results


def check_tables(session: Session) -> list[CheckResult]:
    """Verify all required tables exist."""
    results = []
    required_tables = [
        ("RAW", "TRADES_RAW"),
        ("RAW", "MARKET_EVENTS_RAW"),
        ("RAW", "POSITIONS_RAW"),
        ("STAGE", "TRADES_ENRICHED"),
        ("STAGE", "MARKET_EVENTS_FLATTENED"),
        ("CURATED", "TRADE_METRICS"),
        ("CURATED", "POSITION_SUMMARY"),
    ]

    for schema, table in required_tables:
        try:
            result = session.sql(
                f"SHOW TABLES LIKE '{table}' IN SCHEMA TRADING_LAB_DB.{schema}"
            ).collect()
            if len(result) > 0:
                results.append(
                    CheckResult(f"Table {schema}.{table}", True, "Exists")
                )
            else:
                results.append(
                    CheckResult(f"Table {schema}.{table}", False, "Not found")
                )
        except Exception as e:
            results.append(CheckResult(f"Table {schema}.{table}", False, str(e)))

    return results


def check_streams(session: Session) -> list[CheckResult]:
    """Verify all required streams exist."""
    results = []
    required_streams = [
        "TRADES_RAW_STREAM",
        "MARKET_EVENTS_RAW_STREAM",
        "POSITIONS_RAW_STREAM",
    ]

    try:
        existing = session.sql(
            "SHOW STREAMS IN SCHEMA TRADING_LAB_DB.RAW"
        ).collect()
        existing_names = {row["name"] for row in existing}

        for stream in required_streams:
            if stream in existing_names:
                results.append(CheckResult(f"Stream {stream}", True, "Exists"))
            else:
                results.append(CheckResult(f"Stream {stream}", False, "Not found"))
    except Exception as e:
        for stream in required_streams:
            results.append(CheckResult(f"Stream {stream}", False, str(e)))

    return results


def check_tasks(session: Session) -> list[CheckResult]:
    """Verify all required tasks exist and are running."""
    results = []
    required_tasks = [
        "TASK_TRANSFORM_TRADES",
        "TASK_PUBLISH_CURATED",
        "TASK_UPDATE_POSITIONS",
    ]

    try:
        existing = session.sql(
            "SHOW TASKS IN SCHEMA TRADING_LAB_DB.STAGE"
        ).collect()

        for task in required_tasks:
            task_row = next((r for r in existing if r["name"] == task), None)
            if task_row:
                state = task_row.get("state", "UNKNOWN")
                if state == "started":
                    results.append(
                        CheckResult(f"Task {task}", True, f"Running ({state})")
                    )
                else:
                    results.append(
                        CheckResult(f"Task {task}", False, f"Not running ({state})")
                    )
            else:
                results.append(CheckResult(f"Task {task}", False, "Not found"))
    except Exception as e:
        for task in required_tasks:
            results.append(CheckResult(f"Task {task}", False, str(e)))

    return results


def check_data_loaded(session: Session) -> list[CheckResult]:
    """Check if sample data has been loaded."""
    results = []
    tables = [
        ("RAW", "TRADES_RAW"),
        ("RAW", "MARKET_EVENTS_RAW"),
        ("RAW", "POSITIONS_RAW"),
    ]

    for schema, table in tables:
        try:
            result = session.sql(
                f"SELECT COUNT(*) as cnt FROM TRADING_LAB_DB.{schema}.{table}"
            ).collect()
            count = result[0]["CNT"]
            if count > 0:
                results.append(
                    CheckResult(f"Data in {table}", True, f"{count} rows")
                )
            else:
                results.append(
                    CheckResult(f"Data in {table}", False, "Empty (no data loaded)")
                )
        except Exception as e:
            results.append(CheckResult(f"Data in {table}", False, str(e)))

    return results


def main() -> int:
    """
    Run all verification checks and display results.

    Returns:
        Exit code (0 = all passed, 1 = some failed)
    """
    console = Console()

    console.print("\n[bold blue]Trading Lab Setup Verification[/bold blue]\n")

    # Create session from config
    try:
        session = Session.builder.config("connection_name", "default").create()
    except Exception as e:
        console.print(f"[red]Failed to connect to Snowflake:[/red] {e}")
        return 1

    all_results: list[CheckResult] = []

    try:
        # Run checks
        console.print("[dim]Checking database...[/dim]")
        all_results.append(check_database(session))

        console.print("[dim]Checking schemas...[/dim]")
        all_results.extend(check_schemas(session))

        console.print("[dim]Checking warehouses...[/dim]")
        all_results.extend(check_warehouses(session))

        console.print("[dim]Checking tables...[/dim]")
        all_results.extend(check_tables(session))

        console.print("[dim]Checking streams...[/dim]")
        all_results.extend(check_streams(session))

        console.print("[dim]Checking tasks...[/dim]")
        all_results.extend(check_tasks(session))

        console.print("[dim]Checking data...[/dim]")
        all_results.extend(check_data_loaded(session))

    finally:
        session.close()

    # Display results
    console.print()
    table = Table(title="Verification Results", show_header=True)
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details", style="dim")

    passed = 0
    failed = 0

    for result in all_results:
        status = "[green]PASS[/green]" if result.passed else "[red]FAIL[/red]"
        table.add_row(result.name, status, result.details)
        if result.passed:
            passed += 1
        else:
            failed += 1

    console.print(table)

    # Summary
    console.print()
    if failed == 0:
        console.print(
            f"[bold green]All {passed} checks passed![/bold green] "
            "The Trading Lab demo is ready."
        )
        return 0
    else:
        console.print(
            f"[bold yellow]{passed} passed, {failed} failed.[/bold yellow] "
            "Run the setup SQL scripts to complete configuration."
        )
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())

