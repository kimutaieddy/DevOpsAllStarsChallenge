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

def create_Glue_Database()
    
    try:
        glue_Client.create_database(
            DatabaseInput={
                'Name': MararaNbaDataLake,
                'Description' : "Glue database for NBA sports analytics.",
            }
        )
        print(f"Database'{MararaNbaDataLake}' created successfully")
    except Exception as e:
        print(f"Error creating database: {e}")

def create_glue_table():
    try :
        glue_Client.create_table(
            Database = MararaNbaDataLake,
            TableInput = {
                'Name': 'nba_data',
                'Description': 'NBA data from sportsdata.io',
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': 'game_id',
                            'Type': 'string',
                            'Comment': 'Unique identifier for the game'
                        },
                        {
                            'Name': 'home_team',
                            'Type': 'string',
                            'Comment': 'Home team name'
                        },
                        {
                            'Name': 'away_team',
                            'Type': 'string',
                            'Comment': 'Away team name'
                        },
                        {
                            'Name': 'home_team_score',
                            'Type': 'int',
                            'Comment': 'Home team score'
                        },
                        {
                            'Name': 'away_team_score',
                            'Type': 'int',
                            'Comment': 'Away team score'
                        },
                        {
                            'Name': 'date',
                            'Type': 'string',
                            'Comment': 'Date of the game'
                        }
                    ],
                    'Location': f's3://{Marara_nba_data_lake}/nba_data/',
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'Compressed': False,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        'Parameters': {
                            'separatorChar': ',',
                            'quoteChar': '"',
                            'skip.header.line.count': '1'
                        } ,
                    } ,
                },
            },
        )
        print( f"Glue Table'nba_players' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

def configure_athena() :

    try :
        athena_Client.start_query_execution(
            QueryString = f"CREATE DATABASE IF NOT EXISTS {Marara_nba_data_lake}",
            ResultConfiguration = {
                'OutputLocation': athena_output_location,
            }
        )
        print("Athena configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

    