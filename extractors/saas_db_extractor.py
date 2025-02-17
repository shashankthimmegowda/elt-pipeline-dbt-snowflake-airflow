"""SaaS PostgreSQL database extractor with incremental loading support."""

from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import structlog

from config import get_settings

logger = structlog.get_logger(__name__)

# Tables to extract with their incremental columns
EXTRACTION_CONFIG = {
    "users": {
        "schema": "public",
        "incremental_column": "updated_at",
        "primary_key": "id",
    },
    "orders": {
        "schema": "public",
        "incremental_column": "created_at",
        "primary_key": "id",
    },
    "products": {
        "schema": "public",
        "incremental_column": "updated_at",
        "primary_key": "id",
    },
    "order_items": {
        "schema": "public",
        "incremental_column": "created_at",
        "primary_key": "id",
    },
    "subscriptions": {
        "schema": "public",
        "incremental_column": "updated_at",
        "primary_key": "id",
    },
    "events": {
        "schema": "public",
        "incremental_column": "created_at",
        "primary_key": "id",
    },
}


class SaasDBExtractor:
    """Extracts data from a SaaS PostgreSQL database incrementally."""

    def __init__(self):
        settings = get_settings()
        self.conn_params = {
            "host": settings.saas_db.host,
            "port": settings.saas_db.port,
            "dbname": settings.saas_db.dbname,
            "user": settings.saas_db.user,
            "password": settings.saas_db.password,
        }
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    def _get_connection(self):
        """Create a new database connection."""
        return psycopg2.connect(**self.conn_params)

    def extract_table_full(self, table_name: str) -> list[dict[str, Any]]:
        """Full extraction of a table.

        Args:
            table_name: Name of the table to extract

        Returns:
            List of row dictionaries
        """
        config = EXTRACTION_CONFIG.get(table_name)
        if not config:
            raise ValueError(f"Unknown table: {table_name}")

        schema = config["schema"]
        query = f'SELECT * FROM "{schema}"."{table_name}"'

        logger.info("full_extraction_start", table=table_name)

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()

        records = []
        for row in rows:
            record = {k: self._serialize_value(v) for k, v in dict(row).items()}
            record["_extracted_at"] = self.extracted_at
            record["_source_table"] = table_name
            records.append(record)

        logger.info("full_extraction_complete", table=table_name, rows=len(records))
        return records

    def extract_table_incremental(
        self,
        table_name: str,
        last_extracted_at: str | None = None,
    ) -> list[dict[str, Any]]:
        """Incremental extraction based on a timestamp column.

        Args:
            table_name: Name of the table
            last_extracted_at: ISO timestamp of last successful extraction

        Returns:
            List of new/updated row dictionaries
        """
        config = EXTRACTION_CONFIG.get(table_name)
        if not config:
            raise ValueError(f"Unknown table: {table_name}")

        schema = config["schema"]
        incr_col = config["incremental_column"]

        if last_extracted_at:
            query = f"""
                SELECT * FROM "{schema}"."{table_name}"
                WHERE "{incr_col}" > %s
                ORDER BY "{incr_col}" ASC
            """
            params = (last_extracted_at,)
        else:
            # First run — full extract
            query = f"""
                SELECT * FROM "{schema}"."{table_name}"
                ORDER BY "{incr_col}" ASC
            """
            params = None

        logger.info(
            "incremental_extraction_start",
            table=table_name,
            since=last_extracted_at,
        )

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                rows = cur.fetchall()

        records = []
        for row in rows:
            record = {k: self._serialize_value(v) for k, v in dict(row).items()}
            record["_extracted_at"] = self.extracted_at
            record["_source_table"] = table_name
            record["_extraction_mode"] = "incremental"
            records.append(record)

        logger.info(
            "incremental_extraction_complete",
            table=table_name,
            rows=len(records),
        )
        return records

    def extract_all(
        self,
        mode: str = "incremental",
        watermarks: dict[str, str] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract all configured tables.

        Args:
            mode: 'full' or 'incremental'
            watermarks: Dict of table_name -> last_extracted_at timestamps

        Returns:
            Dict of table_name -> list of records
        """
        watermarks = watermarks or {}
        results = {}

        for table_name in EXTRACTION_CONFIG:
            if mode == "full":
                results[table_name] = self.extract_table_full(table_name)
            else:
                last_ts = watermarks.get(table_name)
                results[table_name] = self.extract_table_incremental(
                    table_name, last_ts
                )

        total_rows = sum(len(v) for v in results.values())
        logger.info(
            "all_tables_extracted",
            mode=mode,
            tables=len(results),
            total_rows=total_rows,
        )
        return results

    def get_table_row_counts(self) -> dict[str, int]:
        """Get row counts for all configured tables (for monitoring)."""
        counts = {}
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for table_name, config in EXTRACTION_CONFIG.items():
                    schema = config["schema"]
                    cur.execute(
                        f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                    )
                    counts[table_name] = cur.fetchone()[0]
        return counts

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        """Serialize Python values for JSON/Snowflake compatibility."""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        return value
