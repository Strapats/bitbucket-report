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
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class BitbucketAPI:
    def __init__(self, max_workers: int = 5, rate_limit_per_second: float = 1.0):
        self.base_url = "https://api.bitbucket.org/2.0"
        self.session = requests.Session()
        self.max_workers = max_workers
        self.rate_limit = rate_limit_per_second
        self._last_request_time = 0
        self._rate_limit_lock = threading.Lock()
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

    def _get_cache_key(self, url: str, params=None) -> str:
        """Generate a cache key from URL and params, ignoring None values."""
        # Get the base key from the URL
        key_parts = [url.split('/')[-2], url.split('/')[-1]]
        
        # Add non-None params to the key
        if params:
            param_str = '_'.join(f"{k}={v}" for k, v in sorted(params.items()) if v is not None)
            if param_str:
                key_parts.append(param_str)
        
        # Generate a unique hash for the full URL and params
        full_key = '_'.join(key_parts)
        hash_key = hashlib.md5(full_key.encode()).hexdigest()[:8]
        
        # Add date to the filename (YYYYMMDD)
        date = datetime.now().strftime('%Y%m%d')
        return f"{key_parts[0]}_{key_parts[1]}_{hash_key}_{date}"

    def _get_cache_path(self, key: str) -> Path:
        """Generate a cache file path for a given key."""
        return self.cache_dir / f"{key}.json"

    def _list_cache_files(self):
        """List all cache files with their keys for debugging."""
        logger.info("Current cache files:")
        for file in self.cache_dir.glob("*.json"):
            logger.info(f"  {file.name}")

    def _cache_response(self, url, params, response_data):
        """Cache response data for a URL."""
        cache_key = self._get_cache_key(url, params)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            with cache_path.open('w') as f:
                json.dump(response_data, f)
            logger.debug(f"💾 Cached data for: {url}")
        except Exception as e:
            logger.warning(f"Failed to write cache file {cache_path}: {e}")

    def _get_cached_response(self, url, params=None, validate_func=None):
        """Get cached response for a URL with optional validation."""
        cache_key = self._get_cache_key(url, params)
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    data = json.load(f)
                    
                    if validate_func:
                        validation_error = validate_func(data)
                        if validation_error:
                            logger.warning(f"⚠ Cache data validation failed for {cache_path.name}: {validation_error}")
                            return None
                        
                    logger.debug(f"✅ Using cached data for: {cache_path.name}")
                    return data
            except Exception as e:
                logger.error(f"❌ Failed to read cache file {cache_path.name}: {e}")
        return None

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache file exists and is not expired."""
        if not cache_path.exists():
            logger.debug(f"Cache file not found: {cache_path.name}")
            return False
            
        try:
            # Extract date from filename (format: name_hash_date.json)
            cache_date_str = cache_path.stem.split('_')[-1]
            cache_date = datetime.strptime(cache_date_str, '%Y%m%d')
            
            # Check if cache is expired (older than cache_expiry)
            if datetime.now() - cache_date > self.cache_expiry:
                logger.debug(f"Cache expired for {cache_path.name}")
                return False
                
            logger.debug(f"Cache not expired {cache_path.name}")
            return True
        except (ValueError, IndexError) as e:
            logger.error(f"❌ Invalid cache filename format: {cache_path.name}")
            return False

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

    def _rate_limit_wait(self):
        """Implement thread-safe rate limiting."""
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - self._last_request_time
            if time_since_last_request < 1.0 / self.rate_limit:
                sleep_time = (1.0 / self.rate_limit) - time_since_last_request
                time.sleep(sleep_time)
            self._last_request_time = time.time()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        max_time=300,
        giveup=lambda e: e.response is not None and e.response.status_code not in [429, 500, 502, 503, 504]
    )
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """Make a rate-limited request with retries."""
        self._rate_limit_wait()
        logger.debug(f"🌐 Making request to: {url}")
        response = self.session.get(url, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 30))
            with self._rate_limit_lock:
                logger.warning(f"🌐 ⚠️ Rate limit hit, waiting {retry_after} seconds")
                time.sleep(retry_after)
                # Adjust rate limit based on response
                self.rate_limit = max(0.5, self.rate_limit * 0.8)  # Reduce rate limit by 20%
                logger.info(f"🌐 Adjusted rate limit to {self.rate_limit} requests/second")
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
        cache_key = self._get_cache_key(url, params)
        cache_path = self._get_cache_path(cache_key)
        
        # Try to get complete paginated data from cache
        if self._is_cache_valid(cache_path):
            try:
                with cache_path.open('r') as f:
                    logger.debug(f"💾 Using cached paginated data for: {url}")
                    return (item for item in json.load(f))
            except Exception as e:
                logger.warning(f"Failed to read cache file {cache_path}: {e}")

        # If not in cache, fetch and store all pages
        logger.info(f"🌐 Making request to: {url}")
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
            logger.debug(f"💾 Cached paginated data for: {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to write cache file {cache_path}: {e}")

        return (item for item in all_items)

    @lru_cache(maxsize=1000)
    def _get_diffstat_cached(self, repo_slug: str, commit_hash: str) -> Dict:
        """Get diffstat for a commit with retries and error handling."""
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit_hash}"
            logger.debug(f"🌐 Fetching diffstat for commit {commit_hash[:8]} in {repo_slug}")
            response = self._make_request(url)
            data = response.json()
            
            # Extract diffstat from response
            if isinstance(data, dict) and 'values' in data:
                diffstat = data['values']
                if isinstance(diffstat, list) and len(diffstat) > 0:
                    total_lines = {'lines_added': 0, 'lines_removed': 0}
                    for stat in diffstat:
                        total_lines['lines_added'] += stat.get('lines_added', 0)
                        total_lines['lines_removed'] += stat.get('lines_removed', 0)
                    return total_lines
                
            logger.warning(f"Unexpected diffstat response format for {commit_hash[:8]}")
            return {'lines_added': 0, 'lines_removed': 0}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Error fetching diffstat for commit {commit_hash[:8]} in {repo_slug}: {str(e)}")
            raise

    def get_repositories(self) -> List[Dict]:
        """Get all repositories for the workspace."""
        logger.info(f"🌐 Fetching repositories for workspace: {config.BITBUCKET_WORKSPACE}")
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}"
            repositories = list(self._paginated_get(url))
            
            if not repositories:
                logger.warning("No repositories found!")
                return []
            
            logger.info(f"🌐 Found {len(repositories)} repositories")
            return repositories
        except Exception as e:
            logger.error(f"🌐 Error fetching repositories: {str(e)}")
            return []

    def get_commits(self, repo_slug: str) -> List[Dict]:
        """Get all commits for a repository."""
        logger.info(f"🌐 Fetching commits for repository: {repo_slug}")
        try:
            url = f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/commits"
            commits = list(self._paginated_get(url))
            
            if not commits:
                logger.warning(f"🌐 No commits found in repository {repo_slug}")
                return []
                        
            logger.info(f"🌐 Retrieved {len(commits)} commits from {repo_slug}")
            return commits
        except Exception as e:
            logger.error(f"🌐 Error fetching commits for {repo_slug}: {str(e)}")
            return []

    def get_diffstats_batch(self, repo_slug: str, commits: List[Dict]) -> List[Dict]:
        """Get diffstats for multiple commits in parallel with retries and caching."""
        total_commits = len(commits)
        results = []
        processed = 0
        chunk_size = 20  # Process in chunks to avoid overwhelming the API
        
        logger.info(f"🌐 Fetching diffstats for {total_commits} commits from {repo_slug}")
        
        # List current cache files for debugging
        self._list_cache_files()
        
        # Process commits in chunks
        for i in range(0, total_commits, chunk_size):
            chunk = commits[i:i + chunk_size]
            chunk_futures = []
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all tasks for this chunk
                for commit in chunk:
                    future = executor.submit(self.fetch_single_diffstat, commit)
                    chunk_futures.append(future)
                
                # Process results as they complete
                for future in as_completed(chunk_futures):
                    result = future.result()
                    results.append(result)
                    processed += 1
                    if processed % 10 == 0:  # Log progress every 10 commits
                        cache_hits = sum(1 for r in results if r.get('from_cache', False))
                        logger.info(f"🌐 Processed {processed}/{total_commits} diffstats ({cache_hits} from cache)")
            
            # Add a small delay between chunks to help prevent rate limiting
            if i + chunk_size < total_commits:
                time.sleep(1)
        
        # Log final processing summary
        successful = sum(1 for r in results if r['success'])
        failed = sum(1 for r in results if not r['success'])
        cache_hits = sum(1 for r in results if r.get('from_cache', False))
        logger.info(f"🌐 ✅ Completed diffstat processing for {repo_slug}: {successful} successful ({cache_hits} from cache), {failed} failed out of {total_commits} total commits")
        
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
            logger.info(f"🌐 Retrieved {len(pull_requests)} pull requests for {repo_slug}")
            return pull_requests
        except requests.RequestException as e:
            logger.error(f"🌐 Error fetching pull requests for {repo_slug}: {e}")
            return []

    def get_diffstat(self, repo_slug: str, commit_hash: str) -> Dict:
        """Get the diffstat for a specific commit."""
        try:
            return self._get_diffstat_cached(repo_slug, commit_hash)
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Error fetching diffstat for commit {commit_hash[:8]} in {repo_slug}: {str(e)}")
            raise

    def validate_diffstat(self, data):
        """Validate diffstat data."""
        logger.debug(f"Validating diffstat")
        if not data:
            return "Empty data"
        data_str = str(data)
        if not ('lines_added' in data_str or 'lines_removed' in data_str):
            return "Missing required fields: lines_added or lines_removed"
        return None  # No error

    def fetch_single_diffstat(self, commit):
        """Fetch diffstat for a single commit with caching."""
        # Get repository slug from commit data
        if isinstance(commit.get('repository'), dict):
            repo_slug = commit['repository'].get('slug') or commit['repository'].get('name')
        else:
            # If repository is not in commit data, try to get it from links
            links = commit.get('links', {})
            if 'html' in links:
                # Extract from URL like "https://bitbucket.org/ascandevelopment/repo-name/commits/hash"
                repo_slug = links['html'].get('href', '').split('/')[4]
            else:
                raise ValueError(f"Could not determine repository for commit {commit['hash'][:8]}")

        cache_key = self._get_cache_key(f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit['hash']}")
        cache_path = self._get_cache_path(cache_key)
        
        cached_data = self._get_cached_response(
            f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit['hash']}",
            validate_func=self.validate_diffstat
        )
        if cached_data:
            return {
                'commit_hash': commit['hash'],
                'diffstat': cached_data,
                'success': True,
                'from_cache': True
            }
                    
        try:
            diffstat = self._get_diffstat_cached(repo_slug, commit['hash'])
            if not diffstat:
                raise ValueError("Empty diffstat response")
                    
            self._cache_response(
                f"{self.base_url}/repositories/{config.BITBUCKET_WORKSPACE}/{repo_slug}/diffstat/{commit['hash']}", 
                None, 
                diffstat
            )
                    
            return {
                'commit_hash': commit['hash'],
                'diffstat': diffstat,
                'success': True
            }
        except Exception as e:
            logger.error(f"🌐 Error fetching diffstat for commit {commit['hash'][:8]}: {str(e)}")
            return {
                'commit_hash': commit['hash'],
                'diffstat': None,
                'success': False,
                'error': str(e)
            }
