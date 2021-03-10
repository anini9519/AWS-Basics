# AWS-Basics
1. Create a S3 Bucket in USA West Region [any availablity zone fine], use aws cli to upload, download, list, delete files in S3
2. Use Python/boto3 library,
	a. to upload a file to S3 from local copy
	b. download files from s3 to local disk
	c. list the files in directory
	d. delete the files in s3 bucket and delete a director in s3 bucket
4. Write a pyspark program, write the e-commerce data set values to RDS [can load small subset like 10K records]
5. Write a pyspark program, Load the RDS data into DataFrames using JDBC [From Problem 4 output], then find the top 3 customers from each country [refer previsous assessments, we have done with CSV already], write the results into Redshift using JDBC
6. Create a database in data catalog for “movielensdb”, create tables for movies, ratings, query them in Athena.
7. Create a database in Redshift called “movies” [no need to create new cluster, do not create new cluster, not in dev db], then create a exernal schema called “movielens” which refers to database “movielensdb”  in problem 6 and create external tables for movies and ratings, perform join to find most popular movies [rated by many users, sum the rating count group by movie id, sort in desc order], most rated movies [avg ratings   group by movie id , sort in desc order], display top 100 results
8. Implement Athena queries to find most popular movies [rated by many users, sum the rating count group by movie id, sort in desc order], most rated movies [avg ratings   group by movie id , sort in desc order], display top 100 results
9. Create a classifier and crawler which can automatically create tables in catalog, update schema if any schema changes based on csv file formats
10. Write a spark program with redshift and JDBC, which execute a join query between Redshift native tables [Users with gender, userid] and redshift spectrum tables [movies, ratings] and perform average rating by a specific gender for a specific movie. 
