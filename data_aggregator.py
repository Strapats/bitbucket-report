import pandas as pd
from datetime import datetime
from typing import Dict, List
import logging
from bitbucket_api import BitbucketAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAggregator:
    def __init__(self, api: BitbucketAPI, year: int):
        self.api = api
        self.year = year
        self.repositories_data = []
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
                    commits = self.api.get_commits(repo_slug, self.year)
                    logger.info(f"Processing {len(commits)} commits for {repo_slug}...")
                    
                    for i, commit in enumerate(commits, 1):
                        if i % 10 == 0:  # Log progress every 10 commits
                            logger.info(f"Progress: {i}/{len(commits)} commits processed for {repo_slug}")
                            
                        commit_data = {
                            'repository': repo_slug,
                            'commit_id': commit['hash'],
                            'date': datetime.strptime(commit['date'][:10], '%Y-%m-%d'),
                            'author': commit['author']['raw'] if commit.get('author') else 'Unknown'
                        }
                        self.commits_data.append(commit_data)
                        
                        try:
                            diffstat = self.api.get_diffstat(repo_slug, commit['hash'])
                            self.file_changes_data.append({
                                'repository': repo_slug,
                                'commit_id': commit['hash'],
                                'date': commit_data['date'],
                                'lines_added': diffstat.get('lines_added', 0),
                                'lines_removed': diffstat.get('lines_removed', 0)
                            })
                        except Exception as e:
                            logger.error(f"Error processing commit {commit['hash'][:8]} in {repo_slug}: {str(e)}")
                            continue
                    
                    logger.info(f"Completed processing {repo_slug}: {len(commits)} commits analyzed")
                    
                    # Collect pull requests
                    prs = self.api.get_pull_requests(repo_slug, self.year)
                    for pr in prs:
                        self.pr_data.append({
                            'repository': repo_slug,
                            'pr_id': pr['id'],
                            'state': pr['state'],
                            'created_on': datetime.strptime(pr['created_on'][:10], '%Y-%m-%d'),
                            'updated_on': datetime.strptime(pr['updated_on'][:10], '%Y-%m-%d')
                        })

                except Exception as e:
                    logger.error(f"Error processing repository {repo_slug}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in data collection: {str(e)}")
            raise

    def aggregate_data(self) -> Dict[str, pd.DataFrame]:
        """Aggregate collected data into DataFrames."""
        # Convert to DataFrames
        commits_df = pd.DataFrame(self.commits_data)
        pr_df = pd.DataFrame(self.pr_data)
        file_changes_df = pd.DataFrame(self.file_changes_data)

        # Add month column
        if not commits_df.empty:
            commits_df['month'] = commits_df['date'].dt.strftime('%Y-%m')
        if not pr_df.empty:
            pr_df['month'] = pr_df['created_on'].dt.strftime('%Y-%m')
        if not file_changes_df.empty:
            file_changes_df['month'] = file_changes_df['date'].dt.strftime('%Y-%m')

        # Monthly aggregations
        monthly_commits = commits_df.groupby(['repository', 'month']).size().reset_index(name='commits')
        monthly_prs = pr_df.groupby(['repository', 'month', 'state']).size().reset_index(name='count')
        monthly_changes = file_changes_df.groupby(['repository', 'month']).agg({
            'lines_added': 'sum',
            'lines_removed': 'sum'
        }).reset_index()

        return {
            'commits': monthly_commits,
            'pull_requests': monthly_prs,
            'file_changes': monthly_changes
        }

    def export_csv(self, data: Dict[str, pd.DataFrame], output_folder: str):
        """Export aggregated data to CSV files."""
        for name, df in data.items():
            filepath = f"{output_folder}/{name}_data.csv"
            df.to_csv(filepath, index=False)
            logger.info(f"Exported {name} data to {filepath}")
