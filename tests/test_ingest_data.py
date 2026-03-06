from unittest.mock import MagicMock, patch
from src import ingest_data

def test_main_calls_spark_flow_without_pyspark(monkeypatch):
    monkeypatch.setenv("INPUT_PATH", "dbfs:/input/")
    monkeypatch.setenv("SCHEMA_LOCATION", "dbfs:/schema/")
    monkeypatch.setenv("CHECKPOINT_LOCATION", "dbfs:/checkpoint/")
    monkeypatch.setenv("TARGET_TABLE", "main.default.raw_ingested_data")

    fake_spark_session = MagicMock()
    spark = MagicMock()
    fake_spark_session.builder.appName.return_value.getOrCreate.return_value = spark

    df = MagicMock()
    query = MagicMock()

    with patch("src.ingest_data.build_stream", return_value=df) as mock_build, \
         patch("src.ingest_data.start_write", return_value=query) as mock_write:
        ingest_data.main(spark_session_cls=fake_spark_session)

    fake_spark_session.builder.appName.assert_called_once_with("IngestionJob")
    mock_build.assert_called_once_with(spark, ingest_data.load_config())
    mock_write.assert_called_once_with(df, ingest_data.load_config())
    query.awaitTermination.assert_called_once()