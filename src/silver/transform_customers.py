from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,trim,upper,initcap,when,lit
)


def transform_customers(df: DataFrame) -> DataFrame:

    df = (df.dropDuplicates(["customer_id"]).withColumn("customer_city",initcap(trim(col("customer_city")))
        )
        .withColumn(
            "customer_state",
            upper(trim(col("customer_state")))
        )
        .withColumn(
            "is_valid",
            when(
                col("customer_id").isNull(),
                lit(False)
            ).otherwise(lit(True))
        )
        .withColumn(
            "dq_reason",
            when(
                col("customer_id").isNull(),
                "customer_id is null"
            )
        )
    )

    return df