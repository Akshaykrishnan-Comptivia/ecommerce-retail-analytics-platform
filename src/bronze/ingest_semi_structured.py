from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from src.bronze.ingest_structured import (
    _landing_path,
    _load_config,
    _qualified_table,
)


def ingest_clickstream(spark):
    df = spark.read.json(
        "/Volumes/ecommerce_analytics_catalog/bronze/raw_data/synthetic/clickstream"
    )

    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(
          "ecommerce_analytics_catalog.bronze.bronze_clickstream_json"
      )

    return "ecommerce_analytics_catalog.bronze.bronze_clickstream_json"


def ingest_amazon_reviews(
    spark: SparkSession,
    config_path: str | None = None,
    mode: str = "overwrite",
) -> str:
    config = _load_config(config_path)
    amazon_cfg = config["bronze"]["public"]["amazon"]
    source_path = (
        f"{_landing_path(config, amazon_cfg['landing_subpath'])}/"
        f"{amazon_cfg['file']}"
    )
    target = _qualified_table(config, amazon_cfg["table"])

    df = spark.read.json(source_path)
    df = df.withColumn("_ingested_at", current_timestamp())
    df.write.format("delta").mode(mode).saveAsTable(target)
    return target
