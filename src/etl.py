import glob

from datetime import datetime
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, dayofmonth, month, year

from models import ingestionSchema, selectedFields


def landing_to_row(logger):

    # the file system is mounted in the dataLake folder so it can be hardcoded
    found_file = glob.glob("dataLake/landing/*.csv")

    if len(found_file) == 0:
        logger.info("No files found in landing zone, nothing to process")

        return

    file_name = found_file[0].split('/')[2].split(".csv")[0]

    # for the sake of simplicity let's assume there is only one file uploaded daily

    target = "dataLake/raw/{0}_{1}.csv".format(
        file_name,
        str(datetime.now().date())
    )

    Path(found_file[0]).rename(target)

    logger.info("moved file {} into raw zone".format(file_name))

    return target


def raw_to_curated(file_path, spark, logger):

    if file_path is None:
        logger.info("Nothing to process")
        return

    file_name = file_path.split('/')[2]

    data = spark.read.csv(
        file_path,
        header=True,
        mode="DROPMALFORMED",
        schema=ingestionSchema,
        dateFormat="dd/MM/yyyy"
    ).select(*selectedFields).withColumn(
        "fileName", lit(file_name)
    ).withColumn(
        "ingestionDate", lit(datetime.now().date())
    ).withColumn(
        "ingestionTime", lit(datetime.now().time().strftime("%H:%M:%S"))
    ).withColumn(
        "orderDay", dayofmonth("OrderDate")
    ).withColumn(
        "orderMonth", month("OrderDate")
    ).withColumn(
        "orderYear", year("OrderDate")
    )

    logger.info("read {0} lines from file {1}".format(data.count(), file_name))

    data.write.partitionBy(
        "orderYear",
        "orderMonth",
        "orderDay"
    ).parquet("/dataLake/curated/transactions")

    return file_name

