from src.common.utils import standardize_column_names
from pyspark.sql.functions import (col,trim,upper,initcap)
from pyspark.sql import DataFrame

def transform_sellers(df: DataFrame) -> DataFrame:

    df = standardize_column_names(df)


    df = df.select("seller_id","seller_zip_code_prefix","seller_city","seller_state","_ingested_at")


    df = (

        df

        .withColumnRenamed(

            "seller_zip_code_prefix",

            "zip_code"

        )

        .withColumnRenamed(

            "seller_city",

            "city"

        )

        .withColumnRenamed(

            "seller_state",

            "state"

        )

    )


    df = (

        df

        .withColumn(

            "city",

            initcap(trim(col("city")))

        )

        .withColumn(

            "state",

            upper(trim(col("state")))

        )

        .withColumn(

            "zip_code",

            trim(col("zip_code"))

        )

    )

    return df