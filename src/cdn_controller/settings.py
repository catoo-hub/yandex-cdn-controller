from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    dry_run: bool = True
    config_path: str = "/app/config.yml"
    database_path: str = "/data/controller.db"
    controller_token: str = ""
    controller_url: str = "http://127.0.0.1:9187"

    yandex_authorized_key_file: str = "/run/secrets/yandex-authorized-key.json"
    cloudflare_api_token: str = ""

    telegram_bot_token: str = ""
    telegram_allowed_user_ids: str = ""
    telegram_chat_ids: str = ""

    uptime_kuma_push_url: str = ""
    generic_webhook_url: str = ""
    generic_webhook_secret: str = ""
    otel_exporter_otlp_endpoint: str = ""

    @property
    def allowed_user_ids(self) -> set[int]:
        return {int(item.strip()) for item in self.telegram_allowed_user_ids.split(",") if item.strip()}

    @property
    def chat_ids(self) -> set[int]:
        return {int(item.strip()) for item in self.telegram_chat_ids.split(",") if item.strip()}
