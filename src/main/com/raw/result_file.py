from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local").appName("races file read the data").getOrCreate()

result_df=spark.read.json("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/results.json")
result_df.printSchema()
result_df.show()