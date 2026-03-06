from unittest.mock import MagicMock
from src.ingest_data import get_env, load_config, build_stream, start_write

from unittest.mock import MagicMock, patch
from src import ingest_data


@patch("src.ingest_data.start_write")
@patch("src.ingest_data.build_stream")
@patch("pyspark.sql.SparkSession")
def test_main_calls_spark_flow(mock_spark_session, mock_build_stream, mock_start_write, monkeypatch):
    monkeypatch.setenv("INPUT_PATH", "dbfs:/input/")
    monkeypatch.setenv("SCHEMA_LOCATION", "dbfs:/schema/")
    monkeypatch.setenv("CHECKPOINT_LOCATION", "dbfs:/checkpoint/")
    monkeypatch.setenv("TARGET_TABLE", "main.default.raw_ingested_data")

    spark = MagicMock()
    mock_spark_session.builder.appName.return_value.getOrCreate.return_value = spark

    df = MagicMock()
    mock_build_stream.return_value = df

    query = MagicMock()
    mock_start_write.return_value = query

    ingest_data.main()

    mock_spark_session.builder.appName.assert_called_once_with("IngestionJob")
    mock_build_stream.assert_called_once()
    mock_start_write.assert_called_once()
    query.awaitTermination.assert_called_once()


def test_get_env_default(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)
    assert get_env("FOO", "bar") == "bar"


def test_get_env_value(monkeypatch):
    monkeypatch.setenv("FOO", "baz")
    assert get_env("FOO", "bar") == "baz"


def test_load_config_defaults(monkeypatch):
    for k in ["INPUT_PATH", "SCHEMA_LOCATION", "CHECKPOINT_LOCATION", "TARGET_TABLE"]:
        monkeypatch.delenv(k, raising=False)

    cfg = load_config()
    assert cfg["input_path"].startswith("dbfs:")
    assert cfg["schema_location"].startswith("dbfs:")
    assert cfg["checkpoint_location"].startswith("dbfs:")
    assert cfg["target_table"]


def test_build_stream_calls_expected_chain():
    spark = MagicMock()

    # spark.readStream.format(...).option(...).load(...)
    read_stream = spark.readStream
    format_obj = read_stream.format.return_value

    # Make option() chainable
    format_obj.option.return_value = format_obj

    cfg = {
        "input_path": "dbfs:/FileStore/ingestion/input/",
        "schema_location": "dbfs:/FileStore/ingestion/schema/",
        "checkpoint_location": "dbfs:/FileStore/ingestion/checkpoint/",
        "target_table": "main.default.raw_ingested_data",
    }

    df = build_stream(spark, cfg)

    read_stream.format.assert_called_once_with("cloudFiles")
    # Verify at least the key options are set
    format_obj.option.assert_any_call("cloudFiles.format", "csv")
    format_obj.option.assert_any_call("cloudFiles.schemaLocation", cfg["schema_location"])
    format_obj.option.assert_any_call("header", "true")
    format_obj.option.assert_any_call("inferColumnTypes", "true")
    format_obj.load.assert_called_once_with(cfg["input_path"])

    # df is whatever load() returns
    assert df == format_obj.load.return_value


def test_start_write_calls_expected_chain():
    df = MagicMock()

    # df.writeStream.format(...).option(...).trigger(...).toTable(...)
    write_stream = df.writeStream
    format_obj = write_stream.format.return_value

    # Make option()/trigger() chainable
    format_obj.option.return_value = format_obj
    format_obj.trigger.return_value = format_obj

    cfg = {
        "checkpoint_location": "dbfs:/FileStore/ingestion/checkpoint/",
        "target_table": "main.default.raw_ingested_data",
    }

    query = start_write(df, cfg)

    write_stream.format.assert_called_once_with("delta")
    format_obj.option.assert_called_once_with("checkpointLocation", cfg["checkpoint_location"])
    format_obj.trigger.assert_called_once_with(availableNow=True)
    format_obj.toTable.assert_called_once_with(cfg["target_table"])

    assert query == format_obj.toTable.return_value