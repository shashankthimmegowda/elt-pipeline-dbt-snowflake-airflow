"""Main ELT Daily Pipeline DAG.

Orchestrates extraction from Reddit, OpenWeather, and SaaS DB,
loads raw data into Snowflake, then triggers dbt transformations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification
from airflow.utils.trigger_rule import TriggerRule

# Default args with Slack failure callback
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": send_slack_webhook_notification(
        slack_webhook_conn_id="slack_webhook",
        text=(
            ":red_circle: *ELT Pipeline Failed*\n"
            "Task: {{ ti.task_id }}\n"
            "DAG: {{ ti.dag_id }}\n"
            "Execution: {{ ts }}\n"
            "Log: {{ ti.log_url }}"
        ),
    ),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def extract_reddit(**context):
    """Extract Reddit data and load to Snowflake."""
    import asyncio
    from extractors import RedditExtractor
    from loaders import SnowflakeLoader

    extractor = RedditExtractor()
    data = extractor.extract_all(posts_per_sub=100, comments_per_post=20)

    loader = SnowflakeLoader()
    results = loader.load_reddit_data(data)

    context["ti"].xcom_push(key="reddit_load_stats", value=results)
    return results


def extract_weather(**context):
    """Extract weather data and load to Snowflake."""
    import asyncio
    from extractors import WeatherExtractor
    from loaders import SnowflakeLoader

    extractor = WeatherExtractor()
    data = asyncio.run(extractor.extract_all())

    loader = SnowflakeLoader()
    results = loader.load_weather_data(data)

    context["ti"].xcom_push(key="weather_load_stats", value=results)
    return results


def extract_saas_db(**context):
    """Extract SaaS DB data incrementally and load to Snowflake."""
    from extractors import SaasDBExtractor
    from loaders import SnowflakeLoader

    # Get watermarks from previous run
    ti = context["ti"]
    watermarks = ti.xcom_pull(
        task_ids="extract_saas_db",
        key="saas_watermarks",
        include_prior_dates=True,
    ) or {}

    extractor = SaasDBExtractor()
    data = extractor.extract_all(mode="incremental", watermarks=watermarks)

    loader = SnowflakeLoader()
    results = loader.load_saas_data(data)

    # Save new watermarks
    from datetime import datetime, timezone
    new_watermarks = {table: datetime.now(timezone.utc).isoformat() for table in data}
    ti.xcom_push(key="saas_watermarks", value=new_watermarks)
    ti.xcom_push(key="saas_load_stats", value=results)

    return results


def quality_check(**context):
    """Run post-load quality checks."""
    from loaders import SnowflakeLoader

    loader = SnowflakeLoader()
    stats = loader.get_load_stats()

    # Check that tables have data
    for source, tables in stats.items():
        for table_info in tables:
            if table_info["rows"] == 0:
                raise ValueError(
                    f"Quality check failed: {source}.{table_info['table']} has 0 rows"
                )

    context["ti"].xcom_push(key="load_stats", value=stats)
    return stats


with DAG(
    dag_id="elt_daily_pipeline",
    default_args=default_args,
    description="Daily ELT pipeline: Extract → Load → Transform (dbt)",
    schedule="0 6 * * *",  # 6 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "production", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    # === EXTRACTION PHASE (parallel) ===
    extract_reddit_task = PythonOperator(
        task_id="extract_reddit",
        python_callable=extract_reddit,
    )

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    extract_saas_task = PythonOperator(
        task_id="extract_saas_db",
        python_callable=extract_saas_db,
    )

    extraction_done = EmptyOperator(
        task_id="extraction_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # === QUALITY CHECK ===
    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    # === DBT TRANSFORMATION PHASE ===
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir . --full-refresh",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --target prod",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir . --target prod",
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir . --target prod",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=send_slack_webhook_notification(
            slack_webhook_conn_id="slack_webhook",
            text=":large_green_circle: *ELT Pipeline Completed Successfully* | {{ ts }}",
        ),
    )

    # === DAG DEPENDENCIES ===
    start >> [extract_reddit_task, extract_weather_task, extract_saas_task]
    [extract_reddit_task, extract_weather_task, extract_saas_task] >> extraction_done
    extraction_done >> quality_check_task
    quality_check_task >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
    dbt_run >> dbt_snapshot
    dbt_test >> dbt_source_freshness >> end
    dbt_snapshot >> end
