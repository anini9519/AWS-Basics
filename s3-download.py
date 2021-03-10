import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('ak-assessment')
s3.Bucket('ak-assessment').download_file('ratings.csv', 'C:/Users/Administrator/Desktop/rats.csv')