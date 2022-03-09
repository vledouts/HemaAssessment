from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.appName('HemaAssessment').getOrCreate()

    data = spark.sparkContext.parallelize([1, 2, 3])

    for el in data.collect():
        print("Hello {} time!".format(el))
