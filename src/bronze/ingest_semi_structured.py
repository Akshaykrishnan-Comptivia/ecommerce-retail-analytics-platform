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