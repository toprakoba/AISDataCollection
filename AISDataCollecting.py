import schedule
import requests
import time
import pandas as pd

def Humlebaek():
    url = "https://www.aishub.net/station/2058/map.json"

    querystring = {"minLat": "55.45884", "maxLat": "55.83854", "minLon": "12.1742", "maxLon": "12.93912",
                   "mode": "type", "zoom": "10", "view": "true", "t": "1662135489"}

    payload = ""
    headers = {
        "authority": "www.aishub.net",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": "_ga=GA1.2.512435699.1661883740; _gid=GA1.2.745750348.1662110901",
        "referer": "https://www.aishub.net/stations/2058",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    data = response.json()
    df = pd.json_normalize(data,["positions"])
    df = df.loc[:, ["ship_name", "lat", "lon", "cog", "sog", "heading", "type"]]
    print(df)

    timestr = time.strftime("%Y%m%d-%H%M%S")
    print(timestr)
    df.to_csv(timestr + ".csv")

from S3Access import bucket_name,access_key, secret_access_key, region_name
import boto3
import os

s3 = boto3.resource(
    service_name='s3',
    region_name= region_name,
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_access_key)

bucket = s3.Bucket(bucket_name)

def S3_Upload_Local_Delete():
    for file in os.listdir():
        if ".csv" in file:
            upload_file_key = "scrapped data/" + str(file)
            bucket.upload_file(file, upload_file_key)
            os.remove(file)


schedule.every(3).minutes.do(S3_Upload_Local_Delete)
schedule.every(3).minutes.do(Humlebaek)

while True:
    schedule.run_pending()
    time.sleep(1)