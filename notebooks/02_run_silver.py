from pyspark.sql import SparkSession
from src.common.utils import deduplicate
from src.silver.transform_customers import transform_customers
from src.silver.transform_products import transform_products
from src.silver.transform_sellers import transform_sellers

spark = SparkSession.builder.getOrCreate()

# Bronze Tables

bronze_customers = spark.table("ecommerce_analytics_catalog.bronze.bronze_customers_csv")
bronze_products = spark.table("ecommerce_analytics_catalog.bronze.bronze_products_csv")
bronze_sellers = spark.table("ecommerce_analytics_catalog.bronze.bronze_sellers_csv")

# Transformations

silver_customers = transform_customers(bronze_customers)
silver_products = transform_products(bronze_products)
silver_sellers = transform_sellers(bronze_sellers)

# Deduplication

silver_customers = deduplicate(silver_customers,["customer_id"])
silver_products = deduplicate(silver_products,["product_id"])
silver_sellers = deduplicate(silver_sellers,["seller_id"])

# Write Silver Tables

(
    silver_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "ecommerce_analytics_catalog.silver.silver_customers"
    )
)

(
    silver_products.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "ecommerce_analytics_catalog.silver.silver_products"
    )
)

(
    silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "ecommerce_analytics_catalog.silver.silver_sellers"
    )
)

print("Silver layer loaded successfully.")
print("Customers :",silver_customers.count())
print("Products :",silver_products.count())
print("Sellers :",silver_sellers.count())