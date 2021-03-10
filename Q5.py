from pyspark.sql.functions import col
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

spark = SparkSession \
        .builder \
        .appName('Python Spark SQL basic example') \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
  
df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://bigdata.cf8emgih4asw.us-east-2.rds.amazonaws.com:5432/ak_productdb') \
        .option('dbtable', 'ecommerce10k') \
        .option('user', 'postgres') \
        .option('password', 'Root12$34') \
        .option('driver', 'org.postgresql.Driver')\
        .load()

df1 = df.select('Country', 'CustomerID', 'UnitPrice')

df2 = df1.filter( col('CustomerID').isNotNull())

df2 = df2.filter( col('UnitPrice') > 0)
df3 = df2.filter( col('Country') != 'Unspecified')
df4 = df3.groupBy(col('CustomerID'),col('Country')).agg(func.sum(col('UnitPrice')).alias("Amount_Spent"))

 
window = Window.partitionBy(df4['Country']).orderBy(df4['Amount_Spent'].desc())
 
df5 = df4.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
#spark.sql('select * from brands').show()
df5.show()
df5.write \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://redshift-cluster-1.cj32ezu2wuro.us-east-2.redshift.amazonaws.com:5439/dev') \
    .option('dbtable', 'ecommerce10k') \
    .option('user', 'awsuser') \
    .option('password', 'Root12$34') \
    .option("driver", "org.postgresql.Driver") \
    .mode("overWrite").save()    
    
