"""Application configuration management."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    source_type: str = "github"

    github_owner: str = "SimplifyJobs"
    github_repo: str = "Summer2026-Internships"
    github_branch: str = "main"
    github_token: str | None = None
    github_target_file: str = "README.md"

    airtable_pat: str | None = None
    airtable_base_id: str | None = None
    airtable_table_id: str | None = None
    airtable_view: str | None = None
    airtable_shared_view_url: str | None = None
    airtable_company_field: str = "Company"
    airtable_role_field: str = "Position Title"
    airtable_location_field: str = "Location"
    airtable_apply_field: str = "Apply"
    airtable_date_field: str = "Date"

    openai_api_key: str
    openai_model: str = "gpt-4o-mini"

    discord_webhook_url: str

    enable_facebook: bool = False
    facebook_page_access_token: str | None = None
    facebook_page_id: str | None = None
    facebook_graph_api_version: str = "v22.0"
    facebook_send_as_dm: bool = False
    facebook_recipient_psid: str | None = None
    facebook_auto_use_captured_psid: bool = True
    facebook_messaging_type: str = "RESPONSE"
    facebook_message_tag: str | None = None
    facebook_webhook_verify_token: str | None = None
    facebook_app_secret: str | None = None

    min_notify_score: int = 75
    database_path: str = "/data/jobs.db"
    request_timeout_seconds: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings."""

    return Settings()
