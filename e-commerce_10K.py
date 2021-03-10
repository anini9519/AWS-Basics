from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('Python Spark SQL basic example') \
        .config('spark.jars', 'postgresql-42.2.19.jar').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.option('header', True).option('inferSchema', True).csv('C:/Users/Administrator/Desktop/data.csv').limit(10000)

df.show(10)

#df.write.format("csv").save("ecommerce_10k")


def toDB(microBatchDf):
    microBatchDf \
        .write \
        .format('jdbc') \
        .option("url", "jdbc:postgresql://bigdata.cf8emgih4asw.us-east-2.rds.amazonaws.com:5432/ak_productdb") \
        .option("dbtable", "ecommerce10k") \
        .option("user", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("password", "Root12$34") \
        .mode("append")\
        .save()

toDB(df)