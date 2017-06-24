import json
import urllib3
import time
from datetime import timedelta
import boto3
import logging
import botocore
import fnmatch
import pandas as pd
import os


json_data = open('config.json')
j_obj = json.load(json_data)

state = j_obj['state']
team = j_obj['team']
link = j_obj['link']
AWSAccess = j_obj['AWSAccess']
AWSSecret = j_obj['AWSSecret']
email = j_obj['notificationEmail']

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

filename = state + '_' + time.strftime("%d%m%Y") + '_WBAN_53138.csv'
# filename = state + '_' + time.strftime("%d%m%Y") + '_WBAN_53138.csv'
print(filename)

http = urllib3.PoolManager()
response = http.request('GET', link)
with open(filename, 'wb') as f:
    f.write(response.data)

with open('link.json', "r") as jsonFile:
    data = json.load(jsonFile)

data[filename] = link

with open('link.json', 'w') as json_file:
    json.dump(data, json_file)


# create connection to Amazon s3
logger.info('Creating connection to Amazon s3!')
client = boto3.client('s3',
                      aws_access_key_id=AWSAccess, 
                      aws_secret_access_key=AWSSecret)
s3 = boto3.resource('s3',
                   aws_access_key_id=AWSAccess, 
                      aws_secret_access_key=AWSSecret)



BucketName = 'Team10_NV_Assignment1'
bucket = s3.Bucket(BucketName)
exists = True
try:
    s3.meta.client.head_bucket(Bucket=BucketName)
except botocore.exceptions.ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        logger.info(BucketName + " does not exist!")
        exists = False
    else:
        logger.info(BucketName + " exists!")
if exists == False:
    logger.info("Creating " + BucketName)
    client.create_bucket(Bucket=BucketName)

s3 = boto3.resource('s3',
                   aws_access_key_id=AWSAccess, 
                      aws_secret_access_key=AWSSecret)
flag = False
oldfilecount = 0
oldfilename = ''
bucket = s3.Bucket(BucketName)
for obj in bucket.objects.all():
    if fnmatch.fnmatch(obj.key, '*_WBAN_53138.csv'):
        oldfilecount += 1
        if obj.key > oldfilename:
            oldfilename = obj.key       
        
    if obj.key == filename:
        logger.info(filename + ' already exists!')
        flag = True
        
# file does not exist(1. no raw file 2. one old raw file)
if flag == False and oldfilecount == 0:
    logger.info(filename + ' does not exists!')
    logger.info('Uploading file!')
    s3.meta.client.upload_file('/Users/XunPeng/Desktop/ADS/docker/notebooks_docker/homework1/' + filename, BucketName, filename)
#2. one old raw file merge new file with old file
elif flag == False and oldfilecount > 0:
    oldfile = pd.read_csv('/Users/XunPeng/Desktop/ADS/docker/notebooks_docker/homework1/'+oldfilename)
    newfile = pd.read_csv(filename)
    file = pd.concat([oldfile, newfile])
    file.to_csv(filename, sep='\t')
    s3.meta.client.upload_file('/Users/XunPeng/Desktop/ADS/docker/notebooks_docker/homework1/' + filename, BucketName, filename)
    
elif flag == True:
    print(filename + ' is existing!')
    
# download file to jupyter file system
logger.info('Downloading ' + filename)
print('Downloading ' + filename)
os.remove(filename)
s3.meta.client.download_file(BucketName, filename, '/Users/XunPeng/Desktop/ADS/docker/notebooks_docker/homework1/' + filename)