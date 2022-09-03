from S3Access import bucket_name,access_key, secret_access_key, region_name
import boto3
import os
import schedule
import time

s3 = boto3.resource(
    service_name='s3',
    region_name= region_name,
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_access_key)

bucket = s3.Bucket(bucket_name)

def S3_Upload():
    for file in os.listdir():
        if ".csv" in file:
            upload_file_key = "scrapped data/" + str(file)
            bucket.upload_file(file, upload_file_key)
            print("Uploaded Succesfully")

schedule.every(5).minutes.do(S3_Upload)
while True:
    schedule.run_pending()
    time.sleep(1)