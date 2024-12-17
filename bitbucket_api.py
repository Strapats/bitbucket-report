import requests
from datetime import datetime, timedelta
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
from pathlib import Path
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitbucketAPI:
    def __init__(self, max_workers: int = 5, rate_limit_per_second: int = 1):
        self.base_url = "https://api.bitbucket.org/2.0"
        self.session = requests.Session()
        self.max_workers = max_workers
        self.rate_limit = rate_limit_per_second
        self._last_request_time = 0
        self.cache_dir = Path('cache')
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_expiry = timedelta(days=1)  # Cache expires after 1 day
        
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

    def _get_cache_path(self, key):
        """Generate a cache file path for a given key."""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key.split('/')[-1]}_{hash_key}.json"

    def _is_cache_valid(self, cache_path):
        """Check if cache file exists and is not expired."""
        if not cache_path.exists():
            return False
        
        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < self.cache_expiry

    def clear_cache(self, older_than_days=None):
        """Clear all or expired cache files.
        
        Args:
            older_than_days (int, optional): If provided, only clear cache files older
                than this many days. If None, clear all cache files.
        """
        if not self.cache_dir.exists():
            return

        current_time = datetime.now()
        for cache_file in self.cache_dir.glob('*.json'):
            file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            should_delete = False
            
            if older_than_days is None:
                should_delete = True
            elif (current_time - file_time).days > older_than_days:
                should_delete = True
                
            if should_delete:
                try:
                    cache_file.unlink()
                    logging.info(f"Cleared cache file: {cache_file.name}")
                except Exception as e:
                    logging.error(f"Failed to clear cache file {cache_file.name}: {e}")

    def _get_cached_response(self, url, params=None):
        """Get cached response for a URL."""
        cache_key = f"{url}_{str(params)}"
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    logging.debug(f"Using cached response for: {url}")
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to read cache file {cache_path}: {e}")
                
        return None

    def _cache_response(self, url, params, response_data):
        """Cache response data for a URL."""
        cache_key = f"{url}_{str(params)}"
        cache_path = self._get_cache_path(cache_key)
        
        try:
            with cache_path.open('w') as f:
                json.dump(response_data, f)
            logging.debug(f"Cached response for: {url}")
        except Exception as e:
            logging.warning(f"Failed to write cache file {cache_path}: {e}")

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
            retry_after = int(response.headers.get('Retry-After', 30))
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

    def _paginated_get(self, url: str, params: Dict = None) -> Generator:
        """Handle paginated API responses."""
        cache_key = f"paginated_{url}_{str(params)}"
        cache_path = self._get_cache_path(cache_key)
        
        # Try to get complete paginated data from cache
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    logger.debug(f"Using cached paginated data for: {url}")
                    return (item for item in json.load(f))
            except Exception as e:
                logger.warning(f"Failed to read cache file {cache_path}: {e}")

        # If not in cache, fetch and store all pages
        logger.info(f"Making request to: {url}")
        all_items = []
        
        while url:
            response = self._make_request(url, params=params)
            if not response.ok:
                self._handle_auth_error(response)
            data = response.json()
            all_items.extend(data.get('values', []))
            url = data.get('next')

        # Cache the complete list
        try:
            with cache_path.open('w') as f:
                json.dump(all_items, f)
            logger.debug(f"Cached paginated data for: {url}")
        except Exception as e:
            logger.warning(f"Failed to write cache file {cache_path}: {e}")

        return (item for item in all_items)

    @lru_cache(maxsize=1000)
    def _get_diffstat_cached(self, repo_slug: str, commit_hash: str) -> Dict:
        """Cached version of diffstat retrieval."""
        url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit_hash}"
        cached_response = self._get_cached_response(url)
        if cached_response:
            return cached_response
        else:
            response = self._make_request(url)
            if not response.ok:
                self._handle_auth_error(response)
            data = response.json()
            self._cache_response(url, None, data)
            return data

    def get_repositories(self) -> List[Dict]:
        """Get all repositories for the workspace."""
        cache_key = f"repositories_{config.BITBUCKET_WORKSPACE}"
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    logger.info("Using cached repository data")
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache file {cache_path}: {e}")

        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}"
            repositories = list(self._paginated_get(url))
            
            # Cache the full repository list
            try:
                with cache_path.open('w') as f:
                    json.dump(repositories, f)
                logger.info("Cached repository data")
            except Exception as e:
                logger.warning(f"Failed to write cache file {cache_path}: {e}")
                
            return repositories
        except Exception as e:
            logger.error(f"Error fetching repositories: {str(e)}")
            return []

    def get_commits(self, repo_slug: str) -> List[Dict]:
        """Get all commits for a repository."""
        cache_key = f"commits_{repo_slug}"
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    logger.info(f"Using cached commit data for {repo_slug}")
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache file {cache_path}: {e}")

        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/commits"
            logger.info(f"Fetching commits for repository: {repo_slug}")
            commits = list(self._paginated_get(url))
            logger.info(f"Retrieved {len(commits)} commits from {repo_slug}")
            
            # Cache the full commit list
            try:
                with cache_path.open('w') as f:
                    json.dump(commits, f)
                logger.info(f"Cached commit data for {repo_slug}")
            except Exception as e:
                logger.warning(f"Failed to write cache file {cache_path}: {e}")
                
            return commits
        except Exception as e:
            logger.error(f"Error fetching commits for {repo_slug}: {str(e)}")
            return []

    def get_diffstats_batch(self, repo_slug: str, commits: List[Dict]) -> List[Dict]:
        """Get diffstats for multiple commits in parallel with retries and caching."""
        logger.info(f"Fetching diffstats for {len(commits)} commits in {repo_slug}")
        results = []
        
        def fetch_single_diffstat(commit):
            cache_key = f"diffstat_{repo_slug}_{commit['hash']}"
            cache_path = self._get_cache_path(cache_key)
            if self._is_cache_valid(cache_path):
                try:
                    with cache_path.open('r') as f:
                        return {
                            'commit_hash': commit['hash'],
                            'diffstat': json.load(f),
                            'success': True
                        }
                except Exception as e:
                    logger.warning(f"Failed to read cache file {cache_path}: {e}")
                    
            try:
                diffstat = self._get_diffstat_cached(repo_slug, commit['hash'])
                self._cache_response(f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit['hash']}", None, diffstat)
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
