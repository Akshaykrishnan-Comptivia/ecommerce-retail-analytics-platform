from src.common.utils import standardize_column_names
from pyspark.sql.functions import (col,trim,initcap)
from pyspark.sql.types import (IntegerType,DecimalType)
from pyspark.sql import DataFrame

def transform_products(df:DataFrame) -> DataFrame:

    df = standardize_column_names(df)



    df = df.select("product_id","product_category_name","product_name_lenght","product_photos_qty","product_weight_g","_ingested_at")


    df = (df

        .withColumnRenamed(
            "product_category_name",
            "category"
        )

        .withColumnRenamed(
            "product_name_lenght",
            "name_length"
        )

        .withColumnRenamed(
            "product_photos_qty",
            "photos_count"
        )

    )


    df = (

        df

        .withColumn(
            "name_length",
            col("name_length").cast(IntegerType())
        )

        .withColumn(
            "photos_count",
            col("photos_count").cast(IntegerType())
        )

        .withColumn(
            "product_weight_g",
            col("product_weight_g").cast(
                DecimalType(10,2)
            )
        )

    )

    df = df.withColumn(

        "weight_kg",

        col("product_weight_g")/1000

    )


    df = df.select("product_id","category","name_length","photos_count","weight_kg","_ingested_at")

    return df