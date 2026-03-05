import os

def get_env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default

def main() -> None:
    # Import Spark only at runtime (keeps unit tests lightweight)
    from pyspark.sql import SparkSession

    input_path = get_env("INPUT_PATH", "dbfs:/FileStore/ingestion/input/")
    schema_location = get_env("SCHEMA_LOCATION", "dbfs:/FileStore/ingestion/schema/")
    checkpoint_location = get_env("CHECKPOINT_LOCATION", "dbfs:/FileStore/ingestion/checkpoint/")
    target_table = get_env("TARGET_TABLE", "main.default.raw_ingested_data")

    print("Starting ingestion...")
    print(f"INPUT_PATH={input_path}")
    print(f"SCHEMA_LOCATION={schema_location}")
    print(f"CHECKPOINT_LOCATION={checkpoint_location}")
    print(f"TARGET_TABLE={target_table}")

    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()

    # Auto Loader (cloudFiles)
    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("header", "true")
            .option("inferColumnTypes", "true")
            .load(input_path)
    )

    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_location)
          .trigger(availableNow=True)
          .toTable(target_table)
    )

    # Important: Jobs must wait for completion
    query.awaitTermination()

    print("Ingestion completed.")

if __name__ == "__main__":
    main()