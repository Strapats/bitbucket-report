import requests
from datetime import datetime
import config
from typing import Dict, List, Optional, Generator
import logging
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitbucketAPI:
    def __init__(self):
        self.base_url = "https://api.bitbucket.org/2.0"
        self.session = requests.Session()
        auth_str = base64.b64encode(
            f"{config.BITBUCKET_USERNAME}:{config.BITBUCKET_APP_PASSWORD}".encode()
        ).decode()
        self.session.headers.update({
            'Authorization': f'Basic {auth_str}',
            'Accept': 'application/json'
        })
        logger.info(f"Initialized BitbucketAPI with workspace: {config.BITBUCKET_WORKSPACE}")
        logger.info(f"Using username: {config.BITBUCKET_USERNAME}")

    def _handle_auth_error(self, response: requests.Response):
        """Handle authentication errors with helpful messages."""
        if response.status_code == 401:
            error_msg = (
                f"\nAuthentication failed! Please check:\n"
                f"1. Your Bitbucket username (currently: {config.BITBUCKET_USERNAME})\n"
                f"2. Your app password has the necessary permissions\n"
                f"3. The app password is correctly copied to the .env file\n"
                f"\nServer response: {response.text}"
            )
            logger.error(error_msg)
            raise requests.exceptions.HTTPError(error_msg, response=response)
        response.raise_for_status()

    def _paginated_get(self, url: str, params: Optional[Dict] = None) -> Generator:
        """Handle paginated API responses."""
        logger.info(f"Making request to: {url}")
        while url:
            response = self.session.get(url, params=params)
            if not response.ok:
                self._handle_auth_error(response)
            data = response.json()
            yield from data.get('values', [])
            url = data.get('next')

    def get_repositories(self) -> List[Dict]:
        """Fetch all repositories in the workspace."""
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}"
            repositories = list(self._paginated_get(url))
            logger.info(f"Retrieved {len(repositories)} repositories")
            return repositories
        except requests.RequestException as e:
            logger.error(f"Error fetching repositories: {e}")
            return []

    def get_commits(self, repo_slug: str) -> List[Dict]:
        """Get all commits for a repository."""
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/commits"
            logger.info(f"Fetching commits for repository: {repo_slug}")
            commits = list(self._paginated_get(url))
            logger.info(f"Retrieved {len(commits)} commits from {repo_slug}")
            return commits
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching commits for {repo_slug}: {str(e)}")
            raise

    def get_pull_requests(self, repo_slug: str, year: int) -> List[Dict]:
        """Fetch pull requests for a specific repository and year."""
        try:
            params = {
                'q': f'created_on >= {year}-01-01 AND created_on < {year+1}-01-01',
                'state': ['MERGED', 'OPEN', 'DECLINED']
            }
            pull_requests = list(self._paginated_get(
                config.get_pull_requests_endpoint(repo_slug),
                params=params
            ))
            logger.info(f"Retrieved {len(pull_requests)} pull requests for {repo_slug}")
            return pull_requests
        except requests.RequestException as e:
            logger.error(f"Error fetching pull requests for {repo_slug}: {e}")
            return []

    def get_diffstat(self, repo_slug: str, commit_hash: str) -> Dict:
        """Get the diffstat for a specific commit."""
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit_hash}"
            logger.debug(f"Fetching diffstat for commit {commit_hash[:8]} in {repo_slug}")
            response = self.session.get(url)
            if not response.ok:
                self._handle_auth_error(response)
            data = response.json()
            logger.debug(f"Retrieved diffstat for commit {commit_hash[:8]}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching diffstat for commit {commit_hash[:8]} in {repo_slug}: {str(e)}")
            raise
