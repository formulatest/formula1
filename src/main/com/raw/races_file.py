from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark=SparkSession.builder.master("local").appName("races file read the data").getOrCreate()

races_df=spark.read.option("header",True).option("inferSchema",True).csv("C:/Users/ARLOKESH/PycharmProjects/formula1/raw/races.csv")
races_df.createOrReplaceTempView("races")

races_sql_df=spark.sql("select * from races")
print(races_sql_df.show())