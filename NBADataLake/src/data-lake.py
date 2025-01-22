import boto3
import json
import time 
import requests
from dotenv import load_dotenv
import os

load_dotenv()

# AWS configuration
region = "us-east-1"
bucket_name = "marara-nba-data-lake"  # Ensure the bucket name is valid
glue_db_name = "marara_nba_data_lake"
athena_output_location = "s3://marara-nba-data-lake/athena_output"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTSDATAIO_API_KEY")
nba_endpoint = os.getenv("SPORTSDATAIO_NBA_ENDPOINT")

if not api_key or not nba_endpoint:
    raise ValueError("API key or NBA endpoint not found in .env file")

# Create AWS clients
s3_Client = boto3.client('s3', region_name=region)
glue_Client = boto3.client('glue', region_name=region)
athena_Client = boto3.client('athena', region_name=region) 

def create_bucket():
    try:
        if region == "us-east-1":
            s3_Client.create_bucket(Bucket=bucket_name)
        else:
            s3_Client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
        print("Bucket created successfully")
    except Exception as e:
        print(f"Error creating bucket: {e}")

def create_glue_database():
    try:
        glue_Client.create_database(
            DatabaseInput={
                'Name': glue_db_name
            }
        )
        print("Glue database created successfully")
    except glue_Client.exceptions.AlreadyExistsException:
        print("Glue database already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")

def create_glue_table():
    try:
        glue_Client.create_table(
            DatabaseName=glue_db_name,
            TableInput={
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
                            'Comment': 'Score of the home team'
                        },
                        {
                            'Name': 'away_team_score',
                            'Type': 'int',
                            'Comment': 'Score of the away team'
                        }
                    ],
                    'Location': f's3://{bucket_name}/nba_data.json',
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
                    }
                }
            }
        )
        print("Glue table created successfully.")
    except glue_Client.exceptions.AlreadyExistsException:
        print("Glue table already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

def fetch_nba_data():
    response = requests.get(nba_endpoint, headers={"Ocp-Apim-Subscription-Key": api_key})
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch NBA data: {response.status_code}")
        return None

def upload_data(s3_client, data):
    try:
        # Convert data to JSON string
        json_data = json.dumps(data)
        # Upload JSON data to S3 bucket
        s3_client.put_object(Bucket=bucket_name, Key='nba_data.json', Body=json_data)
        print("Data uploaded successfully to S3 bucket.")
    except Exception as e:
        print(f"Error uploading data: {e}")

def configure_athena():
    try:
        athena_Client.start_query_execution(
            QueryString=f"CREATE DATABASE IF NOT EXISTS {glue_db_name}",
            ResultConfiguration={
                'OutputLocation': athena_output_location,
            }
        )
        print("Athena configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

# Main function
def main():
    print("Creating NBA Data Lake")
    print("Setting up data lake for NBA sports analytics...")
    time.sleep(5)

    create_bucket()
    create_glue_database()
    nba_data = fetch_nba_data()
    if nba_data:
        upload_data(s3_Client, nba_data)
    create_glue_table()
    configure_athena()

    print("NBA Data Lake setup completed successfully.")

if __name__ == "__main__":
    main()