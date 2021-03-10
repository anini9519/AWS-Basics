from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col


spark=SparkSession \
       .builder \
       .appName("Python Spark SQL basic example") \
       .getOrCreate()

# .config("spark-jars", "postgresql-42.2.14.jar") \

#jdbc:redshift://redshift-cluster-1.cj32ezu2wuro.us-east-2.redshift.amazonaws.com:5439/dev 

# Create a data frame by reading data from SQL Server via JDBC
spark.sparkContext.setLogLevel("WARN")
movieDF = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://redshift-cluster-1.cj32ezu2wuro.us-east-2.redshift.amazonaws.com:5439/movie") \
    .option("dbtable", " movies.movies") \
    .option("user", "awsuser") \
    .option("password", "Root12$34") \
    .option("driver","org.postgresql.Driver") \
    .load()

movieDF.show()
movieDF.createOrReplaceTempView('movies')

 

ratingDF = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://redshift-cluster-1.cj32ezu2wuro.us-east-2.redshift.amazonaws.com:5439/movie") \
    .option("dbtable", "movies.ratings") \
    .option("user", "awsuser") \
    .option("password", "Root12$34") \
    .option("driver", "org.postgresql.Driver") \
    .load()

 
ratingDF.show()
ratingDF.createOrReplaceTempView('ratings')

 

userDF = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://redshift-cluster-1.cj32ezu2wuro.us-east-2.redshift.amazonaws.com:5439/movie") \
    .option("dbtable", "users1") \
    .option("user", "awsuser") \
    .option("password", "Root12$34") \
    .option("driver", "org.postgresql.Driver") \
    .load()

 

userDF.show()
userDF.createOrReplaceTempView('users')

 


gender_avg_rating = ratingDF.alias("r1")\
.join(userDF.alias("u2"), col("r1.userid") == col("u2.userid"))\
.select(col("r1.movieid"), col("u2.gender"), col("r1.rating"))\
.groupBy(col("movieid"), col("gender"))\
.agg(count(col("gender")).alias("count_gender"),avg("rating").alias("avg_rating")) \
.orderBy(desc("avg_rating"))
gender_avg_rating.show()


