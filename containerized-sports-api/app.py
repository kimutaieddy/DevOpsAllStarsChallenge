from flask import flask 
import requests
import json
import os

app = Flask(__name__)

SERP_API_URL = "https://serpapi.com/search.json"
SERP_API_KEY = os.getenv("SPORTS_API_KEY")