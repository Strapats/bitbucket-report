import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List
from bitbucket_api import BitbucketAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAggregator:
    def __init__(self, api: BitbucketAPI, year: int):
        self.api = api
        self.year = year
        self.commits_data = []
        self.pr_data = []
        self.file_changes_data = []

    def collect_data(self):
        """Collect data from Bitbucket API."""
        try:
            repositories = self.api.get_repositories()
            logger.info(f"Processing {len(repositories)} repositories...")

            for repo in repositories:
                repo_slug = repo['slug']
                logger.info(f"\nStarting data collection for repository: {repo_slug}")
                
                try:
                    # Get all commits
                    commits = self.api.get_commits(repo_slug)
                    
                    # Filter commits for the specified year
                    year_commits = [
                        commit for commit in commits 
                        if datetime.strptime(commit['date'][:10], '%Y-%m-%d').year == self.year
                    ]
                    
                    logger.info(f"Processing {len(year_commits)} commits from {self.year} for {repo_slug}")
                    
                    # Process commits in parallel batches
                    if year_commits:
                        diffstats = self.api.get_diffstats_batch(repo_slug, year_commits)
                        
                        # Process results
                        for commit, diffstat_result in zip(year_commits, diffstats):
                            commit_date = datetime.strptime(commit['date'][:10], '%Y-%m-%d')
                            self.commits_data.append({
                                'repository': repo_slug,
                                'commit_hash': commit['hash'],
                                'author': commit['author']['raw'] if commit.get('author') else 'Unknown',
                                'date': commit_date,
                                'month': commit_date.strftime('%Y-%m'),
                                'lines_added': diffstat_result['diffstat'].get('lines_added', 0),
                                'lines_removed': diffstat_result['diffstat'].get('lines_removed', 0)
                            })
                        
                        logger.info(f"Completed processing {repo_slug}: {len(year_commits)} commits analyzed")
                    else:
                        logger.info(f"No commits found for {repo_slug} in {self.year}")
                    
                except Exception as e:
                    logger.error(f"Error processing repository {repo_slug}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in data collection: {str(e)}")
            raise

    def aggregate_data(self) -> Dict[str, pd.DataFrame]:
        """Aggregate collected data into DataFrames."""
        if not self.commits_data:
            logger.warning("No commit data collected to aggregate")
            return {}

        # Convert to DataFrames
        commits_df = pd.DataFrame(self.commits_data)
        
        # Aggregate commits by repository and month
        monthly_commits = commits_df.groupby(['repository', 'month']).agg({
            'commit_hash': 'count',
            'lines_added': 'sum',
            'lines_removed': 'sum'
        }).reset_index()
        
        monthly_commits = monthly_commits.rename(columns={'commit_hash': 'commits'})
        
        # Aggregate by author
        author_stats = commits_df.groupby('author').agg({
            'commit_hash': 'count',
            'lines_added': 'sum',
            'lines_removed': 'sum'
        }).reset_index()
        
        author_stats = author_stats.rename(columns={'commit_hash': 'commits'})
        
        return {
            'monthly_commits': monthly_commits,
            'author_stats': author_stats
        }

    def export_csv(self, data: Dict[str, pd.DataFrame], output_folder: str):
        """Export aggregated data to CSV files."""
        for name, df in data.items():
            filepath = f"{output_folder}/{name}_data.csv"
            df.to_csv(filepath, index=False)
            logger.info(f"Exported {name} data to {filepath}")
