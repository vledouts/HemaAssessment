import sys
import logging

from pyspark.sql import SparkSession

from steps import *

if __name__ == '__main__':

    spark = SparkSession.builder.appName('HemaAssessment').getOrCreate()

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    logger = logging.getLogger('driver_logger')

    logger.info("Hello world!")

    file_path = landing_to_row(logger)

    raw_to_curated(file_path, spark, logger)

