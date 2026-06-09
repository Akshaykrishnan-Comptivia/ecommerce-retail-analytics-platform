from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (col,row_number,desc)

def standardize_column_names(df: DataFrame) -> DataFrame:
    
    for column_name in df.columns:
        new_name = (
            column_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        df = df.withColumnRenamed(column_name, new_name)

    return df


def deduplicate(
    df: DataFrame,
    key_columns: list,
    order_column: str = "_ingested_at") -> DataFrame:
  

    window_spec = (Window.partitionBy(*key_columns).orderBy(desc(order_column)))

    return (df.withColumn("_row_num", row_number().over(window_spec)).filter(col("_row_num") == 1).drop("_row_num"))