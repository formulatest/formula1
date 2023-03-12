from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark=SparkSession.builder.master("local").appName("races file read the data").getOrCreate()

driver_df=spark.read.json("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/drivers.json").withColumn("date_time",current_timestamp())
driver_df.printSchema()
driver_df.show(truncate=False)