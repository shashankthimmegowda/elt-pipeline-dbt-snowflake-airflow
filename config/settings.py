"""Centralized configuration using pydantic-settings."""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class SnowflakeSettings(BaseSettings):
    account: str = Field(..., alias="SNOWFLAKE_ACCOUNT")
    user: str = Field(..., alias="SNOWFLAKE_USER")
    password: str = Field(..., alias="SNOWFLAKE_PASSWORD")
    warehouse: str = Field("ELT_WH", alias="SNOWFLAKE_WAREHOUSE")
    database: str = Field("ELT_DB", alias="SNOWFLAKE_DATABASE")
    role: str = Field("ELT_ROLE", alias="SNOWFLAKE_ROLE")

    model_config = {"env_file": ".env", "extra": "ignore"}


class RedditSettings(BaseSettings):
    client_id: str = Field(..., alias="REDDIT_CLIENT_ID")
    client_secret: str = Field(..., alias="REDDIT_CLIENT_SECRET")
    user_agent: str = Field("elt-pipeline/1.0", alias="REDDIT_USER_AGENT")

    model_config = {"env_file": ".env", "extra": "ignore"}


class WeatherSettings(BaseSettings):
    api_key: str = Field(..., alias="OPENWEATHER_API_KEY")
    base_url: str = "https://api.openweathermap.org/data/2.5"

    model_config = {"env_file": ".env", "extra": "ignore"}


class SaasDBSettings(BaseSettings):
    host: str = Field("localhost", alias="SAAS_DB_HOST")
    port: int = Field(5432, alias="SAAS_DB_PORT")
    dbname: str = Field("saas_app", alias="SAAS_DB_NAME")
    user: str = Field(..., alias="SAAS_DB_USER")
    password: str = Field(..., alias="SAAS_DB_PASSWORD")

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


class SlackSettings(BaseSettings):
    webhook_url: str = Field("", alias="SLACK_WEBHOOK_URL")

    model_config = {"env_file": ".env", "extra": "ignore"}


class Settings(BaseSettings):
    snowflake: SnowflakeSettings = SnowflakeSettings()
    reddit: RedditSettings = RedditSettings()
    weather: WeatherSettings = WeatherSettings()
    saas_db: SaasDBSettings = SaasDBSettings()
    slack: SlackSettings = SlackSettings()

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()
