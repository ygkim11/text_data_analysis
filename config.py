from dotenv import load_dotenv 
import os

load_dotenv()

ELASTIC_SEARCH_URL=os.getenv("ELASTIC_SEARCH_URL")
ELASTIC_SEARCH_ID=os.getenv("ELASTIC_SEARCH_ID")
ELASTIC_SEARCH_PW=os.getenv("ELASTIC_SEARCH_PW")

ELASTIC_SEARCH_AUTH = (ELASTIC_SEARCH_ID,ELASTIC_SEARCH_PW)