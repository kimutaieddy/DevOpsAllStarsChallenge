import boto3
import json
import time 
import requests
from dotenv import load_dotenv
import os

load_dotenv()

# AWS configuration
region ="us-east-1"
bucket_name = "Marara_nba-data-lake"
glue_db_name = "Marara_nba_data_lake"
athena_output_location = "s3://Marara_nba-data-lake/athena_output"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTSDATAIO_API_KEY")
nba_endpoint = os.getenv("SPORTSDATAIO_NBA_ENDPOINT")

# Create AWS clients
s3_Client = boto3.client('s3', region_name=region)
glue_Client = boto3.client('glue', region_name=region)
athena_Client = boto3.client('athena', region_name=region) 

def create_bucket():

    try:
        if region == "us-east-1":
            s3_Client.create_bucket(Bucket=Marara_nba_data_lake)
        else:
            s3_Client.create_bucket(Bucket=Marara_nba_data_lake,CreateBucketConfiguration={'LocationConstraint': region})
            print("Bucket created successfully")
    except Exception as e:
        print(f"Error creating bucket: {e}")
