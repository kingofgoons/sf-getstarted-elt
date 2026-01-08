"""
Tests for SQL file syntax validation.

Verifies that SQL files:
- Are readable
- Have valid structure
- Contain expected statements
"""

import re
from pathlib import Path

import pytest


def read_sql_file(path: Path) -> str:
    """Read SQL file contents."""
    return path.read_text()


def extract_statements(sql: str) -> list[str]:
    """
    Extract SQL statements from a file.
    
    Handles:
    - Single-line comments (--)
    - Multi-line comments (/* */)
    - Statement separators (;)
    """
    # Remove multi-line comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Split by semicolon, filter empty
    statements = []
    for stmt in sql.split(';'):
        # Remove single-line comments for statement detection
        lines = []
        for line in stmt.split('\n'):
            # Keep line if not a pure comment
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                lines.append(line)
        
        clean_stmt = '\n'.join(lines).strip()
        if clean_stmt:
            statements.append(clean_stmt)
    
    return statements


class TestSQLFilesExist:
    """Verify all expected SQL files exist."""

    def test_setup_sql_exists(self, sql_dir: Path) -> None:
        """Verify 00_setup.sql exists."""
        assert (sql_dir / "00_setup.sql").exists()

    def test_stages_formats_sql_exists(self, sql_dir: Path) -> None:
        """Verify 01_stages_formats.sql exists."""
        assert (sql_dir / "01_stages_formats.sql").exists()

    def test_streams_demo_sql_exists(self, sql_dir: Path) -> None:
        """Verify 02a_streams_demo.sql exists."""
        assert (sql_dir / "02a_streams_demo.sql").exists()

    def test_transform_demo_sql_exists(self, sql_dir: Path) -> None:
        """Verify 02b_transform_demo.sql exists."""
        assert (sql_dir / "02b_transform_demo.sql").exists()

    def test_tasks_demo_sql_exists(self, sql_dir: Path) -> None:
        """Verify 02c_tasks_demo.sql exists."""
        assert (sql_dir / "02c_tasks_demo.sql").exists()

    def test_snowpark_procedure_sql_exists(self, sql_dir: Path) -> None:
        """Verify 02d_snowpark_procedure.sql exists."""
        assert (sql_dir / "02d_snowpark_procedure.sql").exists()

    def test_dbt_setup_sql_exists(self, sql_dir: Path) -> None:
        """Verify 03_dbt_setup.sql exists."""
        assert (sql_dir / "03_dbt_setup.sql").exists()

    def test_cost_monitoring_sql_exists(self, sql_dir: Path) -> None:
        """Verify 04_cost_monitoring.sql exists."""
        assert (sql_dir / "04_cost_monitoring.sql").exists()

    def test_advanced_optional_sql_exists(self, sql_dir: Path) -> None:
        """Verify 05_advanced_optional.sql exists."""
        assert (sql_dir / "05_advanced_optional.sql").exists()


class TestSetupSQL:
    """Tests for 00_setup.sql."""

    @pytest.fixture
    def setup_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "00_setup.sql")

    def test_creates_role(self, setup_sql: str) -> None:
        """Verify role creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?ROLE\s+TRADING_LAB_ROLE', 
                        setup_sql, re.IGNORECASE)

    def test_creates_database(self, setup_sql: str) -> None:
        """Verify database creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?DATABASE\s+TRADING_LAB_DB', 
                        setup_sql, re.IGNORECASE)

    def test_creates_schemas(self, setup_sql: str) -> None:
        """Verify schema creation."""
        schemas = ['RAW', 'STAGE', 'CURATED', 'ANALYTICS']
        for schema in schemas:
            pattern = rf'CREATE\s+(OR\s+REPLACE\s+)?SCHEMA\s+.*{schema}'
            assert re.search(pattern, setup_sql, re.IGNORECASE), \
                f"Missing schema creation for {schema}"

    def test_creates_warehouses(self, setup_sql: str) -> None:
        """Verify warehouse creation."""
        warehouses = ['TRADING_INGEST_WH', 'TRADING_TRANSFORM_WH', 'TRADING_ANALYTICS_WH']
        for wh in warehouses:
            pattern = rf'CREATE\s+(OR\s+REPLACE\s+)?WAREHOUSE\s+{wh}'
            assert re.search(pattern, setup_sql, re.IGNORECASE), \
                f"Missing warehouse creation for {wh}"

    def test_grants_to_role(self, setup_sql: str) -> None:
        """Verify grants to role."""
        assert re.search(r'GRANT\s+.*TO\s+ROLE\s+TRADING_LAB_ROLE', 
                        setup_sql, re.IGNORECASE)


class TestStagesFormatsSQL:
    """Tests for 01_stages_formats.sql."""

    @pytest.fixture
    def stages_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "01_stages_formats.sql")

    def test_creates_file_formats(self, stages_sql: str) -> None:
        """Verify file format creation."""
        formats = ['FF_CSV_TRADES', 'FF_JSON_EVENTS', 'FF_PARQUET_POSITIONS']
        for fmt in formats:
            pattern = rf'CREATE\s+(OR\s+REPLACE\s+)?FILE\s+FORMAT\s+{fmt}'
            assert re.search(pattern, stages_sql, re.IGNORECASE), \
                f"Missing file format {fmt}"

    def test_creates_internal_stage(self, stages_sql: str) -> None:
        """Verify internal stage creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+RAW_INTERNAL_STAGE', 
                        stages_sql, re.IGNORECASE)

    def test_creates_external_s3_stage(self, stages_sql: str) -> None:
        """Verify external S3 stage creation with storage integration."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+RAW_S3_STAGE', 
                        stages_sql, re.IGNORECASE)
        assert re.search(r'STORAGE_INTEGRATION\s*=\s*S3_INT', 
                        stages_sql, re.IGNORECASE)
        assert re.search(r"URL\s*=\s*'s3://", stages_sql, re.IGNORECASE)

    def test_creates_tables(self, stages_sql: str) -> None:
        """Verify table creation."""
        tables = ['TRADES_RAW', 'MARKET_EVENTS_RAW', 'POSITIONS_RAW']
        for table in tables:
            pattern = rf'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+{table}'
            assert re.search(pattern, stages_sql, re.IGNORECASE), \
                f"Missing table {table}"

    def test_has_copy_statements(self, stages_sql: str) -> None:
        """Verify COPY INTO statements exist."""
        assert re.search(r'COPY\s+INTO\s+TRADES_RAW', stages_sql, re.IGNORECASE)
        assert re.search(r'COPY\s+INTO\s+MARKET_EVENTS_RAW', stages_sql, re.IGNORECASE)
        assert re.search(r'COPY\s+INTO\s+POSITIONS_RAW', stages_sql, re.IGNORECASE)

    def test_copy_uses_s3_stage(self, stages_sql: str) -> None:
        """Verify COPY statements reference S3 stage."""
        assert re.search(r'FROM\s+@RAW_S3_STAGE', stages_sql, re.IGNORECASE)


class TestStreamsDemoSQL:
    """Tests for 02a_streams_demo.sql."""

    @pytest.fixture
    def streams_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "02a_streams_demo.sql")

    def test_creates_stream(self, streams_sql: str) -> None:
        """Verify stream creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?STREAM\s+TRADES_RAW_STREAM', 
                        streams_sql, re.IGNORECASE)

    def test_demonstrates_stream_has_data(self, streams_sql: str) -> None:
        """Verify SYSTEM$STREAM_HAS_DATA is demonstrated."""
        assert re.search(r'SYSTEM\$STREAM_HAS_DATA', streams_sql, re.IGNORECASE)


class TestTransformDemoSQL:
    """Tests for 02b_transform_demo.sql."""

    @pytest.fixture
    def transform_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "02b_transform_demo.sql")

    def test_creates_stage_tables(self, transform_sql: str) -> None:
        """Verify STAGE tables are created."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+TRADES_ENRICHED', 
                        transform_sql, re.IGNORECASE)

    def test_creates_curated_tables(self, transform_sql: str) -> None:
        """Verify CURATED tables are created."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+TRADE_METRICS', 
                        transform_sql, re.IGNORECASE)


class TestTasksDemoSQL:
    """Tests for 02c_tasks_demo.sql."""

    @pytest.fixture
    def tasks_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "02c_tasks_demo.sql")

    def test_creates_transform_task(self, tasks_sql: str) -> None:
        """Verify transform task creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?TASK\s+TASK_TRANSFORM_TRADES', 
                        tasks_sql, re.IGNORECASE)

    def test_creates_aggregate_task(self, tasks_sql: str) -> None:
        """Verify aggregate task creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?TASK\s+TASK_AGGREGATE_METRICS', 
                        tasks_sql, re.IGNORECASE)

    def test_task_uses_stream_condition(self, tasks_sql: str) -> None:
        """Verify task uses SYSTEM$STREAM_HAS_DATA."""
        assert re.search(r'SYSTEM\$STREAM_HAS_DATA', tasks_sql, re.IGNORECASE)

    def test_task_chaining(self, tasks_sql: str) -> None:
        """Verify task chaining with AFTER."""
        assert re.search(r'AFTER\s+TASK_TRANSFORM_TRADES', tasks_sql, re.IGNORECASE)

    def test_resumes_tasks(self, tasks_sql: str) -> None:
        """Verify tasks are resumed."""
        assert re.search(r'ALTER\s+TASK\s+.*RESUME', tasks_sql, re.IGNORECASE)


class TestSnowparkProcedureSQL:
    """Tests for 02d_snowpark_procedure.sql."""

    @pytest.fixture
    def snowpark_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "02d_snowpark_procedure.sql")

    def test_creates_procedure(self, snowpark_sql: str) -> None:
        """Verify stored procedure creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+SP_TRANSFORM_TRADES', 
                        snowpark_sql, re.IGNORECASE)

    def test_uses_python(self, snowpark_sql: str) -> None:
        """Verify procedure uses Python."""
        assert re.search(r'LANGUAGE\s+PYTHON', snowpark_sql, re.IGNORECASE)

    def test_uses_snowpark_package(self, snowpark_sql: str) -> None:
        """Verify Snowpark package is included."""
        assert re.search(r'snowflake-snowpark-python', snowpark_sql, re.IGNORECASE)

    def test_uses_fully_qualified_names(self, snowpark_sql: str) -> None:
        """Verify uses fully qualified table names (not use_database)."""
        # Should NOT have use_database or use_schema in Python code
        # But SHOULD have fully qualified names like TRADING_LAB_DB.RAW.TRADES_RAW_STREAM
        assert re.search(r'TRADING_LAB_DB\.RAW\.TRADES_RAW_STREAM', snowpark_sql)
        assert re.search(r'TRADING_LAB_DB\.STAGE\.TRADES_ENRICHED', snowpark_sql)


class TestDBTSetupSQL:
    """Tests for 03_dbt_setup.sql."""

    @pytest.fixture
    def dbt_sql(self, sql_dir: Path) -> str:
        return read_sql_file(sql_dir / "03_dbt_setup.sql")

    def test_creates_dbt_role(self, dbt_sql: str) -> None:
        """Verify DBT role creation."""
        assert re.search(r'CREATE\s+(OR\s+REPLACE\s+)?ROLE\s+DBT_TRADING_ROLE', 
                        dbt_sql, re.IGNORECASE)

    def test_grants_select_on_curated(self, dbt_sql: str) -> None:
        """Verify SELECT grant on CURATED schema."""
        assert re.search(r'GRANT\s+SELECT\s+ON.*CURATED', dbt_sql, re.IGNORECASE)

    def test_grants_create_on_analytics(self, dbt_sql: str) -> None:
        """Verify CREATE grant on ANALYTICS schema."""
        assert re.search(r'GRANT\s+CREATE\s+TABLE\s+ON\s+SCHEMA.*ANALYTICS', 
                        dbt_sql, re.IGNORECASE)

