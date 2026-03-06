from src.ingest_data import load_config

def test_load_config_defaults(monkeypatch):
    monkeypatch.delenv("INPUT_PATH", raising=False)
    monkeypatch.delenv("SCHEMA_LOCATION", raising=False)
    monkeypatch.delenv("CHECKPOINT_LOCATION", raising=False)
    monkeypatch.delenv("TARGET_TABLE", raising=False)

    cfg = load_config()
    assert "input_path" in cfg
    assert "schema_location" in cfg
    assert "checkpoint_location" in cfg
    assert "target_table" in cfg