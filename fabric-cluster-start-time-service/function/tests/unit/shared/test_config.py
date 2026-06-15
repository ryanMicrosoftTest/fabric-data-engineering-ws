from shared.config import Settings, get_settings


def test_target_workspace_id_list_splits():
    s = Settings(target_workspace_ids="a, b , ,c")
    assert s.target_workspace_id_list == ["a", "b", "c"]


def test_target_workspace_id_list_empty():
    s = Settings(target_workspace_ids="")
    assert s.target_workspace_id_list == []


def test_defaults():
    s = Settings()
    assert s.fabric_api_base_url == "https://api.fabric.microsoft.com"
    assert s.mi_client_id is None


def test_env_var_loading(monkeypatch):
    monkeypatch.setenv("FABRIC_TENANT_ID", "tid-123")
    monkeypatch.setenv("WAREHOUSE_SQL_ENDPOINT", "ep")
    monkeypatch.setenv("WAREHOUSE_DATABASE", "db")
    monkeypatch.setenv("TARGET_WORKSPACE_IDS", "ws-1,ws-2")
    s = Settings()
    assert s.fabric_tenant_id == "tid-123"
    assert s.warehouse_sql_endpoint == "ep"
    assert s.warehouse_database == "db"
    assert s.target_workspace_id_list == ["ws-1", "ws-2"]


def test_get_settings_cached():
    a = get_settings()
    b = get_settings()
    assert a is b
