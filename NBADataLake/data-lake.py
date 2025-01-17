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