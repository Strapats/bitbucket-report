import requests
from datetime import datetime
import config
from typing import Dict, List, Optional, Generator
import logging
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import time
import json
import os
import backoff

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitbucketAPI:
    def __init__(self, max_workers: int = 5, rate_limit_per_second: int = 1):
        self.base_url = "https://api.bitbucket.org/2.0"
        self.session = requests.Session()
        self.max_workers = max_workers
        self.rate_limit = rate_limit_per_second
        self._last_request_time = 0
        self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Debug logging for environment variables
        logger.info("Environment variables:")
        logger.info(f"WORKSPACE: {config.BITBUCKET_WORKSPACE}")
        logger.info(f"USERNAME: {config.BITBUCKET_USERNAME}")
        logger.info(f"APP_PASSWORD length: {len(config.BITBUCKET_APP_PASSWORD) if config.BITBUCKET_APP_PASSWORD else 0}")
        
        if not all([config.BITBUCKET_WORKSPACE, config.BITBUCKET_USERNAME, config.BITBUCKET_APP_PASSWORD]):
            raise ValueError("Missing required environment variables. Please check your .env file.")
        
        auth_str = base64.b64encode(
            f"{config.BITBUCKET_USERNAME}:{config.BITBUCKET_APP_PASSWORD}".encode()
        ).decode()
        self.session.headers.update({
            'Authorization': f'Basic {auth_str}',
            'Accept': 'application/json'
        })
        
        logger.info(f"Initialized BitbucketAPI with workspace: {config.BITBUCKET_WORKSPACE}")
        logger.info(f"Using username: {config.BITBUCKET_USERNAME}")

    def _get_cache_path(self, cache_key: str) -> str:
        """Get the cache file path for a given key."""
        return os.path.join(self.cache_dir, f"{cache_key}.json")

    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Try to get data from cache."""
        cache_path = self._get_cache_path(cache_key)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error reading cache file {cache_path}: {e}")
        return None

    def _save_to_cache(self, cache_key: str, data: Dict):
        """Save data to cache."""
        cache_path = self._get_cache_path(cache_key)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Error writing to cache file {cache_path}: {e}")

    def _rate_limit_wait(self):
        """Implement rate limiting."""
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        if time_since_last_request < 1.0 / self.rate_limit:
            time.sleep(1.0 / self.rate_limit - time_since_last_request)
        self._last_request_time = time.time()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code not in [429, 500, 502, 503, 504]
    )
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """Make a rate-limited request with retries."""
        self._rate_limit_wait()
        response = self.session.get(url, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning(f"Rate limit hit, waiting {retry_after} seconds")
            time.sleep(retry_after)
            return self._make_request(url, params)
        response.raise_for_status()
        return response

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
            response = self._make_request(url, params=params)
            if not response.ok:
                self._handle_auth_error(response)
            data = response.json()
            yield from data.get('values', [])
            url = data.get('next')

    @lru_cache(maxsize=1000)
    def _get_diffstat_cached(self, repo_slug: str, commit_hash: str) -> Dict:
        """Cached version of diffstat retrieval."""
        url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit_hash}"
        response = self._make_request(url)
        if not response.ok:
            self._handle_auth_error(response)
        return response.json()

    def get_repositories(self) -> List[Dict]:
        """Get all repositories for the workspace."""
        cache_key = f"repositories_{config.BITBUCKET_WORKSPACE}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.info("Using cached repository data")
            return cached_data

        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}"
            repositories = list(self._paginated_get(url))
            self._save_to_cache(cache_key, repositories)
            return repositories
        except Exception as e:
            logger.error(f"Error fetching repositories: {str(e)}")
            raise

    def get_commits(self, repo_slug: str) -> List[Dict]:
        """Get all commits for a repository."""
        cache_key = f"commits_{repo_slug}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.info(f"Using cached commit data for {repo_slug}")
            return cached_data

        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/commits"
            logger.info(f"Fetching commits for repository: {repo_slug}")
            commits = list(self._paginated_get(url))
            logger.info(f"Retrieved {len(commits)} commits from {repo_slug}")
            self._save_to_cache(cache_key, commits)
            return commits
        except Exception as e:
            logger.error(f"Error fetching commits for {repo_slug}: {str(e)}")
            raise

    def get_diffstats_batch(self, repo_slug: str, commits: List[Dict]) -> List[Dict]:
        """Get diffstats for multiple commits in parallel with retries and caching."""
        logger.info(f"Fetching diffstats for {len(commits)} commits in {repo_slug}")
        results = []
        
        def fetch_single_diffstat(commit):
            cache_key = f"diffstat_{repo_slug}_{commit['hash']}"
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                return {
                    'commit_hash': commit['hash'],
                    'diffstat': cached_data,
                    'success': True
                }

            try:
                diffstat = self._get_diffstat_cached(repo_slug, commit['hash'])
                self._save_to_cache(cache_key, diffstat)
                return {
                    'commit_hash': commit['hash'],
                    'diffstat': diffstat,
                    'success': True
                }
            except Exception as e:
                logger.error(f"Error fetching diffstat for {commit['hash'][:8]}: {str(e)}")
                return {
                    'commit_hash': commit['hash'],
                    'diffstat': {'lines_added': 0, 'lines_removed': 0},
                    'success': False
                }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_commit = {
                executor.submit(fetch_single_diffstat, commit): commit 
                for commit in commits
            }
            
            completed = 0
            for future in as_completed(future_to_commit):
                completed += 1
                if completed % 10 == 0:
                    logger.info(f"Progress: {completed}/{len(commits)} diffstats retrieved")
                results.append(future.result())

        logger.info(f"Completed fetching diffstats for {repo_slug}")
        return results

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
            return self._get_diffstat_cached(repo_slug, commit_hash)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching diffstat for commit {commit_hash[:8]} in {repo_slug}: {str(e)}")
            raise
