from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,trim,when,lit)

def transform_products(df: DataFrame) -> DataFrame:

    df = (df.dropDuplicates(["product_id"])
          .withColumn("product_category_name",trim(col("product_category_name")))
        .withColumn(
            "is_valid",
            when(col("product_id").isNull(), False)
            .when(col("product_weight_g") <= 0, False)
            .when(col("product_length_cm") <= 0, False)
            .when(col("product_height_cm") <= 0, False)
            .when(col("product_width_cm") <= 0, False)
            .otherwise(True)
        )
        .withColumn(
            "dq_reason",
            when(
                col("product_id").isNull(),
                "product_id is null"
            )
            .when(
                col("product_weight_g") <= 0,
                "invalid weight"
            )
            .when(
                col("product_length_cm") <= 0,
                "invalid length"
            )
            .when(
                col("product_height_cm") <= 0,
                "invalid height"
            )
            .when(
                col("product_width_cm") <= 0,
                "invalid width"
            )
        )
    )

    return df