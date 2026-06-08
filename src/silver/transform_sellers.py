from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,trim,upper,initcap,when,lit)

def transform_sellers(df: DataFrame) -> DataFrame:

    df = (
        df
        .dropDuplicates(["seller_id"])
        .withColumn(
            "seller_city",
            initcap(trim(col("seller_city")))
        )
        .withColumn(
            "seller_state",
            upper(trim(col("seller_state")))
        )
        .withColumn(
            "is_valid",
            when(
                col("seller_id").isNull(),
                False
            ).otherwise(True)
        )
        .withColumn(
            "dq_reason",
            when(
                col("seller_id").isNull(),
                "seller_id is null"
            )
        )
    )

    return df