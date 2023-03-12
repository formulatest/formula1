from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

spark=SparkSession.builder.master("local").appName("races file read the data").getOrCreate()

circuit_df=spark.read.option("header",True).option("inferSchema",True).csv("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/circuits.csv")
races_df=spark.read.option("header",True).option("inferSchema",True).csv("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/races.csv")

join_df=circuit_df.join(races_df,circuit_df.circuitId==races_df.circuitId,"inner")

print(join_df.show())

# join_df.createOrReplaceTempView("data")
#
# spark_sql_df=spark.sql("select * from data")
# print(spark_sql_df.show(truncate=False))
collect_df=circuit_df.collect()
print(collect_df)
datacollect=races_df.select("name").show(truncate=False)
print(datacollect)

split_df=datacollect.withColumn("firstname",split(col("name"),",").getItem(0)).withColumn("middlename",split(col("name"),",").getItem(1)).witholumn("lastname",split(col("name"),",").getItem(2))
print(split_df.show())

