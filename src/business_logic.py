from datetime import timedelta
from pyspark.sql.functions import col, count, max, split

from models import customerKeys


def generate_sales(file_to_load, spark, logger):

    data = spark.read.parquet(
        "/dataLake/curated/transactions"
    ).where(
        "fileName='{}'".format(file_to_load)
    ).select(
        "orderId",
        "orderDate",
        "shipDate",
        "shipMode",
        "city",
        "orderDay",
        "orderMonth",
        "orderYear"
    )

    data.write.mode("overwrite").partitionBy(
        "orderYear",
        "orderMonth",
        "orderDay"
    ).parquet("/dataLake/consumption/Sales")

    logger.info("Wrote {} lines into Sales dataset".format(data.count()))

    return


def get_days_stats(df, days):

    # let's imagine we're on the date of the last order

    ref_date = df.select(max(col("orderDate")).alias("ref")).collect()[0]['ref']

    return df.where(
        col("orderDate") >= (ref_date - timedelta(days=days))
    ).groupBy(*customerKeys).agg(
        count("orderId").alias(
            "quantityOfOrdersLast{}Days".format(str(days))
        )
    )


def generate_customers(spark, logger):

    data = spark.read.parquet("/dataLake/curated/transactions")

    staging = data.select(
        col("customerId"),
        col("customerName"),
        split(col("customerName"), ' ').getItem(0).alias("customerFirstName"),
        split(col("customerName"), ' ').getItem(1).alias("customerLastName"),
        col("Segment").alias("customerSegment"),
        col("country"),
        col("city"),
        col("orderDate"),
        col("orderId")
    ).cache()

    total_orders = staging.groupBy(*customerKeys).agg(
        count("orderId").alias("totalQuantityOfOrders")
    )

    stats = total_orders.join(
        get_days_stats(staging, 5),
        on=customerKeys,
        how="left"
    ).join(
        get_days_stats(staging, 15),
        on=customerKeys,
        how="left"
    ).join(
        get_days_stats(staging, 30),
        on=customerKeys,
        how="left"
    ).fillna(0)

    stats.write.mode("overwrite").parquet("/dataLake/consumption/Customers")

    logger.info("Wrote stats for {} customers".format(stats.count()))

    return
