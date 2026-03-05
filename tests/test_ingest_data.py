import os
from src.ingest_data import get_env

def test_get_env_returns_default_when_missing(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)
    assert get_env("FOO", "bar") == "bar"

def test_get_env_returns_value_when_set(monkeypatch):
    monkeypatch.setenv("FOO", "baz")
    assert get_env("FOO", "bar") == "baz"

def test_get_env_strips_whitespace(monkeypatch):
    monkeypatch.setenv("FOO", "  hello  ")
    assert get_env("FOO", "bar") == "hello"