# Import the necessary modules
from pyspark.sql import SparkSession

if __name__ == "__main__":
   # Create a SparkSession
   spark = SparkSession.builder \
      .appName("My App") \
      .getOrCreate()

   rdd = spark.sparkContext.parallelize(range(1, 100))

   print("THE SUM IS HERE: ", rdd.sum())
   # Stop the SparkSession
   spark.stop()