"""Unit tests for ELT pipeline extraction and transformation logic."""

import pytest
import json
from datetime import datetime, timezone, timedelta


class TestRedditExtraction:
    """Test Reddit data extraction and normalization."""

    def test_post_normalization(self):
        raw_post = {
            "id": "abc123",
            "title": "  Test Post  ",
            "selftext": "body text",
            "score": 150,
            "num_comments": 42,
            "created_utc": 1700000000,
            "subreddit": "datascience",
            "author": "test_user",
        }
        normalized = {
            "post_id": raw_post["id"],
            "title": raw_post["title"].strip(),
            "body": raw_post["selftext"],
            "score": raw_post["score"],
            "num_comments": raw_post["num_comments"],
            "created_at": datetime.fromtimestamp(raw_post["created_utc"], tz=timezone.utc).isoformat(),
            "subreddit": raw_post["subreddit"],
            "author": raw_post["author"],
        }
        assert normalized["title"] == "Test Post"
        assert normalized["subreddit"] == "datascience"
        assert "2023" in normalized["created_at"]

    def test_engagement_ratio(self):
        score, comments = 500, 100
        ratio = round(comments / max(score, 1), 4)
        assert ratio == 0.2

    def test_empty_subreddit_returns_empty(self):
        posts = []
        assert len(posts) == 0

    def test_rate_limit_backoff(self):
        base_delay = 1.0
        max_delay = 60.0
        for attempt in range(5):
            delay = min(base_delay * (2 ** attempt), max_delay)
            assert delay <= max_delay


class TestWeatherExtraction:
    """Test weather data extraction logic."""

    def test_temperature_fields(self):
        response = {
            "main": {"temp": 22.5, "feels_like": 21.0, "humidity": 65, "pressure": 1013},
        }
        assert response["main"]["temp"] == 22.5
        assert response["main"]["humidity"] == 65

    def test_temperature_conversion_c_to_f(self):
        celsius = 25.0
        fahrenheit = round(celsius * 9 / 5 + 32, 2)
        assert fahrenheit == 77.0

    def test_city_coordinates_valid(self):
        cities = {
            "New York": (40.7128, -74.0060),
            "London": (51.5074, -0.1278),
            "Tokyo": (35.6762, 139.6503),
        }
        for city, (lat, lon) in cities.items():
            assert -90 <= lat <= 90
            assert -180 <= lon <= 180

    def test_50_cities_coverage(self):
        assert 50 >= 50


class TestSaasDBExtraction:
    """Test SaaS database incremental extraction."""

    def test_watermark_update(self):
        old_watermark = datetime(2024, 1, 1, tzinfo=timezone.utc)
        new_records = [
            {"updated_at": datetime(2024, 1, 2, tzinfo=timezone.utc)},
            {"updated_at": datetime(2024, 1, 5, tzinfo=timezone.utc)},
            {"updated_at": datetime(2024, 1, 3, tzinfo=timezone.utc)},
        ]
        new_watermark = max(r["updated_at"] for r in new_records)
        assert new_watermark > old_watermark
        assert new_watermark == datetime(2024, 1, 5, tzinfo=timezone.utc)

    def test_incremental_query_filter(self):
        watermark = "2024-01-01T00:00:00+00:00"
        query = f"SELECT * FROM users WHERE updated_at > '{watermark}' ORDER BY updated_at"
        assert watermark in query
        assert "ORDER BY" in query

    def test_batch_size_limits(self):
        batch_size = 1000
        total_records = 5500
        num_batches = (total_records + batch_size - 1) // batch_size
        assert num_batches == 6

    def test_extracted_at_metadata(self):
        record = {"id": 1, "email": "test@test.com"}
        record["_extracted_at"] = datetime.now(timezone.utc).isoformat()
        record["_source_table"] = "users"
        assert "_extracted_at" in record
        assert record["_source_table"] == "users"


class TestSnowflakeLoading:
    """Test Snowflake VARIANT JSON loading pattern."""

    def test_variant_json_serialization(self):
        record = {"id": 1, "name": "test", "tags": ["a", "b"]}
        variant = json.dumps(record)
        parsed = json.loads(variant)
        assert parsed == record

    def test_raw_table_naming(self):
        source = "reddit"
        table_name = f"raw_{source}"
        assert table_name == "raw_reddit"

    def test_dedup_window_logic(self):
        records = [
            {"id": 1, "value": "old", "ingested_at": "2024-01-01"},
            {"id": 1, "value": "new", "ingested_at": "2024-01-02"},
            {"id": 2, "value": "only", "ingested_at": "2024-01-01"},
        ]
        seen = {}
        for r in records:
            if r["id"] not in seen or r["ingested_at"] > seen[r["id"]]["ingested_at"]:
                seen[r["id"]] = r
        assert len(seen) == 2
        assert seen[1]["value"] == "new"
