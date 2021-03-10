import boto3

# Let's use Amazon S3
s3 = boto3.client('s3')

#bucket = s3.Bucket('ak-assessment')
s3.upload_file('C:/Users/Administrator/Desktop/Testfile.txt', 'ak-assessment', 'S3_order.txt')


