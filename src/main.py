import sys
import logging

from pyspark.sql import SparkSession

from etl import landing_to_row, raw_to_curated
from business_logic import generate_sales

if __name__ == '__main__':

    spark = SparkSession.builder.appName('HemaAssessment').getOrCreate()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    logger = logging.getLogger('driver_logger')

    file_path = landing_to_row(logger)

    loaded_file = raw_to_curated(file_path, spark, logger)

    generate_sales(loaded_file, spark, logger)
