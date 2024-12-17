import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bitbucket API Configuration
BITBUCKET_WORKSPACE = os.getenv('BITBUCKET_WORKSPACE')
BITBUCKET_USERNAME = os.getenv('BITBUCKET_USERNAME')
BITBUCKET_APP_PASSWORD = os.getenv('BITBUCKET_APP_PASSWORD')

# API Base URL
BITBUCKET_API_BASE_URL = 'https://api.bitbucket.org/2.0'

# Report Configuration
REPORT_YEAR = datetime.now().year
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), 'output')

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# API Endpoints
def get_repositories_endpoint():
    return f'{BITBUCKET_API_BASE_URL}/repositories/{BITBUCKET_WORKSPACE}'

def get_commits_endpoint(repo_slug):
    return f'{BITBUCKET_API_BASE_URL}/repositories/{BITBUCKET_WORKSPACE}/{repo_slug}/commits'

def get_pull_requests_endpoint(repo_slug):
    return f'{BITBUCKET_API_BASE_URL}/repositories/{BITBUCKET_WORKSPACE}/{repo_slug}/pullrequests'

def get_diffstat_endpoint(repo_slug, commit_id):
    return f'{BITBUCKET_API_BASE_URL}/repositories/{BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit_id}'
