import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List
from bitbucket_api import BitbucketAPI

logging.basicConfig(level=logging.INFO)

class DataAggregator:
    """Class to aggregate data from Bitbucket API."""

    def __init__(self, api: BitbucketAPI, year: int = None):
        """Initialize with API client and optional year filter."""
        self.api = api
        self.year = year
        self.total_commits = 0
        self.processed_commits = 0
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def collect_data(self, year: int = None) -> Dict[str, pd.DataFrame]:
        """Collect all data from Bitbucket."""
        repositories = self.api.get_repositories()
        self.logger.info(f"Found {len(repositories)} repositories")
        self.logger.debug(f"Repositories structure: {repositories}")
        self.logger.debug(f"Type of repositories: {type(repositories)}")
    
        # First pass to count total commits for progress tracking
        self.logger.info("Counting total commits...")
        for repo in repositories:
            try:
                self.logger.debug(f"Processing repository: {repo['slug']}")
                commits = self.api.get_commits(repo['slug'])
                
                if year or self.year:
                    commits = [c for c in commits if datetime.fromisoformat(c['date'].replace('Z', '+00:00')).year == (year or self.year)]
                self.total_commits += len(commits)
            except Exception as e:
                self.logger.error(f"Error processing commits for {repo['slug']}: {str(e)}", exc_info=True)
                continue
        self.logger.info(f"Total commits to process: {self.total_commits}")

        # Process repositories and collect data
        all_commits_data = []
        all_diffstats_data = []
        
        for i, repo in enumerate(repositories, 1):
            self.logger.info(f"Processing repository {i}/{len(repositories)}: {repo['slug']}")
            
            try:
                # Get commits
                commits = self.api.get_commits(repo['slug'])
                if not commits:
                    self.logger.warning(f"No commits found for repository {repo['slug']}")
                    continue

                if year or self.year:
                    commits = [c for c in commits if datetime.fromisoformat(c['date'].replace('Z', '+00:00')).year == (year or self.year)]
                
                # Log first commit structure for debugging
                if commits:
                    self.logger.debug(f"First commit structure: {commits[0]}")
                
                # Get diffstats
                diffstats = self.api.get_diffstats_batch(repo['slug'], commits)
                
                # Process commits
                for commit in commits:
                    try:
                        commit_date = datetime.fromisoformat(commit['date'].replace('Z', '+00:00'))
                        author = commit.get('author', {})
                        author_raw = author.get('raw') if isinstance(author, dict) else str(author)
                        
                        commit_data = {
                            'repository': repo['slug'],
                            'commit_hash': commit['hash'],
                            'author': author_raw,
                            'date': commit_date,
                            'month': commit_date.strftime('%Y-%m'),
                            'message': commit.get('message', '')
                        }
                        all_commits_data.append(commit_data)
                    except Exception as e:
                        self.logger.error(f"Error processing commit {commit.get('hash', 'unknown')}: {str(e)}")
                        self.logger.debug(f"Problematic commit data: {commit}")
                        continue
                
                # Process diffstats
                for stat in diffstats:
                    if stat['success'] and stat['diffstat']:
                        all_diffstats_data.append({
                            'repository': repo['slug'],
                            'commit_hash': stat['commit_hash'],
                            'lines_added': stat['diffstat'].get('lines_added', 0),
                            'lines_removed': stat['diffstat'].get('lines_removed', 0)
                        })
                
                self.processed_commits += len(commits)
                self.logger.info(f"Overall progress: {self.processed_commits}/{self.total_commits} commits processed ({(self.processed_commits/self.total_commits*100):.1f}%)")
                
            except Exception as e:
                self.logger.error(f"Error processing repository {repo['slug']}: {str(e)}")
                continue

        # Convert to DataFrames
        commits_df = pd.DataFrame(all_commits_data)
        diffstats_df = pd.DataFrame(all_diffstats_data)
        
        self.logger.info(f"Collected data for {len(commits_df)} commits and {len(diffstats_df)} diffstats")
        
        return {
            'commits': commits_df,
            'diffstats': diffstats_df
        }
