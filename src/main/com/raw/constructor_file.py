from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark=SparkSession.builder.master("local").appName("races file read the data").getOrCreate()

constructor_df=spark.read.json("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/constructors.json").withColumn("date_time",current_timestamp())
constructor_df.printSchema()
constructor_df.show(truncate=False)

