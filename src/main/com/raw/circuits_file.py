from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark=SparkSession.builder.master("local").appName("circuit file read the data").getOrCreate()

circuit_df=spark.read.option("header",True).option("inferSchema",True).csv("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/circuits.csv").withColumn("date_time",current_timestamp())
circuit_df.createOrReplaceTempView("circuit")
circuit_sql_df=spark.sql("select * from circuit")
print(circuit_sql_df.show())