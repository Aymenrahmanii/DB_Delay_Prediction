"""Settings, read from environment variables prefixed DBDELAY_ (or a .env file)."""

from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

BERLIN = ZoneInfo("Europe/Berlin")

DEFAULT_BASE_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DBDELAY_", env_file=".env", extra="ignore")

    db_client_id: SecretStr = SecretStr("")
    db_api_key: SecretStr = SecretStr("")
    base_url: str = DEFAULT_BASE_URL

    data_dir: Path = Path("data")
    stations_file: Path | None = None  # None = the bundled list

    rate_limit_per_minute: int = Field(default=50, ge=1)
    plan_rate_limit_per_minute: int = Field(default=10, ge=1)
    request_timeout_s: float = 20.0
    max_attempts: int = Field(default=4, ge=1)

    # Polling schedule (seconds).
    fchg_interval_s: int = 300
    rchg_interval_s: int = 60
    plan_check_interval_s: int = 600
    plan_lookahead_hours: int = Field(default=3, ge=0, le=12)

    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def poll_log_dir(self) -> Path:
        return self.data_dir / "polls"
