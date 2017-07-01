import json
import urllib.request
import time
from datetime import timedelta
import boto3
import logging
import botocore
import fnmatch
import sys
import pandas as pd

filename = 'configWrangle.json'

with open(filename, "r") as jsonFile:
    data = json.load(jsonFile)

# data["link"] = "New link1"
state = data['state']
team = data['team']
AWSAccess = data['AWSAccess']
AWSSecret = data['AWSSecret']


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logfile = time.strftime("%Y-%m-%d_%H:%M:%S" + ".log")

# create a file handler
handler = logging.FileHandler(logfile)
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)

logger.info('Hello!!!!')



# create connection to Amazon s3
client = boto3.client('s3',
                      aws_access_key_id=AWSAccess, 
                      aws_secret_access_key=AWSSecret)
s3 = boto3.resource('s3',
                   aws_access_key_id=AWSAccess, 
                      aws_secret_access_key=AWSSecret)

BucketName = 'Team10_NV_Assignment1'
bucket = s3.Bucket(BucketName)
rawfilename = ''

# find the newest raw file in bucket and download the file to local system
for obj in bucket.objects.all():
    if fnmatch.fnmatch(obj.key, '*_WBAN_53138.csv'):
        if obj.key > rawfilename:
            rawfilename = obj.key
if rawfilename == '':
    logger.info('the raw data does not exist! errors!')
    sys.exit("the raw data does not exist! errors!")
cleanfilename = state + '_' + time.strftime("%d%m%Y") + '_WBAN_53138_clean.csv'



# check clean file exists or not
flag = False
for obj in bucket.objects.all():
    if obj.key == cleanfilename:
        flag = True

# if it exists  do not do anything
# if it does not exist just upload the file
if flag == False:
    logger.info(cleanfilename + ' does not exist in bucket')
    s3.meta.client.download_file(BucketName, rawfilename, cleanfilename)
    logger.info('Downloading raw file from bucket to local system')
    #clear data
    


    df = pd.read_csv(cleanfilename)

    columnsToClean = ['HOURLYDRYBULBTEMPF', 'HOURLYDRYBULBTEMPC', 'HOURLYWindSpeed', 'HOURLYPrecip']

    df[columnsToClean] = df[columnsToClean].astype(str)

    for col in columnsToClean:
        df[col] = [''.join([c for c in val if c in '1234567890.']) for val in df[col]]

    df.to_csv(cleanfilename)
    
    
    
    
    
    logger.info('Cleaning data!')
    
    s3.meta.client.upload_file(cleanfilename, BucketName, cleanfilename)
    logger.info('upload cleaned file to bucket')
else:
    logger.info(cleanfilename + 'exist in bucket!')
# delete the old one
# oldfile = ''
# for obj in bucket.objects.all():
#     if fnmatch.fnmatch(obj.key, '*_WBAN_53138_clean.csv'):
#         oldfile = obj.key
# print(oldfile)
# client.delete_object(Bucket=BucketName, Key=oldfile)

# upload new file

# update links in json
data['rawData'] = 'https://s3.amazonaws.com/' + BucketName + '/' + rawfilename
data['cleanData'] = 'https://s3.amazonaws.com/' + BucketName + '/' + cleanfilename
with open(filename, "w") as jsonFile:
    json.dump(data, jsonFile)

# Send Email

ACCESS_KEY = '*******************'
SECRET_ACCESS_KEY = '*******************'
REGION_NAME = '*******************'

s3Session = boto3.Session(
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_ACCESS_KEY,
    region_name = REGION_NAME
)

client = s3Session.client('ses', region_name='us-west-2')
email = 'liu.jiah@husky.neu.edu'

try:
    client.send_email(
        Destination={
            'ToAddresses': [email]
        },
        Message = {
            'Subject': {
                'Data': "Your Job is Done."
            }, 
            'Body': {
                'Text': {
                    'Data': 'Cong! Your Job is Done.'
                }
            }
        },
        Source = email
    )
except Exception as e:
    print(e)
