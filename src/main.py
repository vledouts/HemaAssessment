from pyspark.sql import SparkSession

from steps import *

if __name__ == '__main__':

    spark = SparkSession.builder.appName('HemaAssessment').getOrCreate()

    logger4j = spark.sparkContext._jvm.org.apache.log4j

    logger = logger4j.logManager.getLogger(__name__)

    logger.info("Hello world!")

    landing_to_row(logger)

