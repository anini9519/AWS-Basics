import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('ak-assessment')
for s3_object  in bucket.objects.all():
    print(s3_object.key)