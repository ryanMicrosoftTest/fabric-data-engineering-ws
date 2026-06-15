from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, case_sensitive=False, extra="ignore")

    fabric_tenant_id: str = ""
    fabric_api_base_url: str = "https://api.fabric.microsoft.com"
    warehouse_sql_endpoint: str = ""
    warehouse_database: str = ""
    mi_client_id: str | None = None
    target_workspace_ids: str = ""
    applicationinsights_connection_string: str | None = None

    @property
    def target_workspace_id_list(self) -> list[str]:
        if not self.target_workspace_ids:
            return []
        return [w.strip() for w in self.target_workspace_ids.split(",") if w.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
