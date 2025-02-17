"""Snowflake data loader with support for raw JSON and structured loading."""

import json
from datetime import datetime, timezone
from typing import Any

import snowflake.connector
import structlog

from config import get_settings

logger = structlog.get_logger(__name__)

# Schema configuration
RAW_SCHEMA = "RAW"
SCHEMAS = {
    "reddit": f"{RAW_SCHEMA}.RAW_REDDIT",
    "weather": f"{RAW_SCHEMA}.RAW_WEATHER",
    "air_quality": f"{RAW_SCHEMA}.RAW_WEATHER",
    "saas": f"{RAW_SCHEMA}.RAW_SAAS",
}


class SnowflakeLoader:
    """Loads extracted data into Snowflake raw schemas."""

    def __init__(self):
        settings = get_settings()
        self.conn_params = {
            "account": settings.snowflake.account,
            "user": settings.snowflake.user,
            "password": settings.snowflake.password,
            "warehouse": settings.snowflake.warehouse,
            "database": settings.snowflake.database,
            "role": settings.snowflake.role,
        }
        self.loaded_at = datetime.now(timezone.utc).isoformat()

    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create a new Snowflake connection."""
        return snowflake.connector.connect(**self.conn_params)

    def _ensure_raw_table(self, conn, schema: str, table_name: str) -> None:
        """Create raw table if it doesn't exist (variant column pattern)."""
        conn.cursor().execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                raw_data VARIANT,
                loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                source_file VARCHAR(500),
                batch_id VARCHAR(100)
            )
        """)

    def load_json_records(
        self,
        records: list[dict[str, Any]],
        source_type: str,
        table_name: str,
        batch_id: str | None = None,
    ) -> int:
        """Load records as JSON into a Snowflake raw table.

        Uses the VARIANT column pattern — each record is stored as a single
        JSON object in the raw_data column for maximum flexibility.

        Args:
            records: List of dictionaries to load
            source_type: Key from SCHEMAS dict (reddit, weather, saas)
            table_name: Target table name
            batch_id: Optional batch identifier for lineage

        Returns:
            Number of rows loaded
        """
        if not records:
            logger.warning("no_records_to_load", source=source_type, table=table_name)
            return 0

        schema = SCHEMAS.get(source_type, f"{RAW_SCHEMA}.RAW_{source_type.upper()}")
        batch_id = batch_id or f"{source_type}_{self.loaded_at}"

        logger.info(
            "loading_records",
            source=source_type,
            table=table_name,
            count=len(records),
            batch_id=batch_id,
        )

        conn = self._get_connection()
        try:
            self._ensure_raw_table(conn, schema, table_name)

            cursor = conn.cursor()
            insert_sql = f"""
                INSERT INTO {schema}.{table_name} (raw_data, loaded_at, source_file, batch_id)
                SELECT PARSE_JSON(%s), %s, %s, %s
            """

            # Batch insert in chunks of 1000
            chunk_size = 1000
            loaded = 0

            for i in range(0, len(records), chunk_size):
                chunk = records[i : i + chunk_size]
                rows = [
                    (
                        json.dumps(record, default=str),
                        self.loaded_at,
                        f"{source_type}/{table_name}",
                        batch_id,
                    )
                    for record in chunk
                ]
                cursor.executemany(insert_sql, rows)
                loaded += len(chunk)
                logger.debug("chunk_loaded", loaded=loaded, total=len(records))

            conn.commit()
            logger.info(
                "load_complete",
                source=source_type,
                table=table_name,
                rows_loaded=loaded,
            )
            return loaded

        except Exception as e:
            conn.rollback()
            logger.error(
                "load_failed",
                source=source_type,
                table=table_name,
                error=str(e),
            )
            raise
        finally:
            conn.close()

    def load_reddit_data(self, data: dict[str, list[dict]]) -> dict[str, int]:
        """Load Reddit extraction results."""
        results = {}
        if data.get("posts"):
            results["posts"] = self.load_json_records(
                data["posts"], "reddit", "POSTS"
            )
        if data.get("comments"):
            results["comments"] = self.load_json_records(
                data["comments"], "reddit", "COMMENTS"
            )
        return results

    def load_weather_data(self, data: dict[str, list[dict]]) -> dict[str, int]:
        """Load weather extraction results."""
        results = {}
        if data.get("current_weather"):
            results["current_weather"] = self.load_json_records(
                data["current_weather"], "weather", "CURRENT_WEATHER"
            )
        if data.get("air_quality"):
            results["air_quality"] = self.load_json_records(
                data["air_quality"], "air_quality", "AIR_QUALITY"
            )
        return results

    def load_saas_data(self, data: dict[str, list[dict]]) -> dict[str, int]:
        """Load SaaS DB extraction results."""
        results = {}
        for table_name, records in data.items():
            results[table_name] = self.load_json_records(
                records, "saas", table_name.upper()
            )
        return results

    def get_load_stats(self) -> dict[str, Any]:
        """Get loading statistics for monitoring."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            stats = {}
            for source, schema in SCHEMAS.items():
                cursor.execute(f"""
                    SELECT
                        TABLE_NAME,
                        ROW_COUNT,
                        BYTES,
                        LAST_ALTERED
                    FROM {self.conn_params['database']}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema.split('.')[-1]}'
                    ORDER BY TABLE_NAME
                """)
                stats[source] = [
                    {
                        "table": row[0],
                        "rows": row[1],
                        "bytes": row[2],
                        "last_altered": row[3].isoformat() if row[3] else None,
                    }
                    for row in cursor.fetchall()
                ]
            return stats
        finally:
            conn.close()

    def truncate_and_reload(self, schema: str, table_name: str) -> None:
        """Truncate a raw table for full reload scenarios."""
        conn = self._get_connection()
        try:
            conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {schema}.{table_name}")
            conn.commit()
            logger.info("table_truncated", schema=schema, table=table_name)
        finally:
            conn.close()
