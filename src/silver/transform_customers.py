from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,trim,upper,initcap,lit)
from src.common.utils import standardize_column_names


def transform_customers(df: DataFrame) -> DataFrame:
 
    df = standardize_column_names(df)

    df = df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "_ingested_at"
    )

    df = (
        df
        .withColumnRenamed("customer_zip_code_prefix", "zip_code")
        .withColumnRenamed("customer_city", "city")
        .withColumnRenamed("customer_state", "state")
        
    )


    df = (
        df
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("zip_code", trim(col("zip_code")))
        .withColumn("country", lit("Brazil"))
    )

    return df