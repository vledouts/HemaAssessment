

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

    data.write.partitionBy(
        "orderYear",
        "orderMonth",
        "orderDay"
    ).parquet("/dataLake/consumption/Sales")

    logger.info("Wrote {} lines into Sales dataset".format(data.count()))

    return

