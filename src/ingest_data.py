import argparse
import os


def get_env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default="dbfs:/FileStore/ingestion/input/")
    parser.add_argument("--schema-location", default="dbfs:/FileStore/ingestion/schema/")
    parser.add_argument("--checkpoint-location", default="dbfs:/FileStore/ingestion/checkpoint/")
    parser.add_argument("--target-table", default="main.default.raw_ingested_data")
    args, _ = parser.parse_known_args()
    return args


def load_config(args) -> dict:
    return {
        "input_path": args.input_path,
        "schema_location": args.schema_location,
        "checkpoint_location": args.checkpoint_location,
        "target_table": args.target_table,
    }


def build_stream(spark, cfg: dict):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", cfg["schema_location"])
            .option("header", "true")
            .option("inferColumnTypes", "true")
            .load(cfg["input_path"])
    )


def start_write(df, cfg: dict):
    return (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", cfg["checkpoint_location"])
          .trigger(availableNow=True)
          .toTable(cfg["target_table"])
    )


def main(spark_session_cls=None) -> None:
    if spark_session_cls is None:
        from pyspark.sql import SparkSession
        spark_session_cls = SparkSession

    args = parse_args()
    cfg = load_config(args)
    spark = spark_session_cls.builder.appName("IngestionJob").getOrCreate()
    df = build_stream(spark, cfg)
    query = start_write(df, cfg)
    query.awaitTermination()


if __name__ == "__main__":  # pragma: no cover
    main()