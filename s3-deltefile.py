import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('ak-assessment')
s21 = boto3.client('s3')

s21.delete_object(Bucket='ak-assessment', Key='movies.csv')
