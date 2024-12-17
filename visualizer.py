import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict
import os
import logging
import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, data: Dict[str, pd.DataFrame], output_folder: str):
        self.data = data
        self.output_folder = output_folder
        os.makedirs(output_folder, exist_ok=True)
        
        # Set up seaborn style
        sns.set_style("whitegrid")
        sns.set_palette("husl")

    def create_monthly_activity_chart(self):
        """Create line chart showing commits, PRs, and file changes over months."""
        plt.figure(figsize=(12, 6))
        
        # Plot commits
        commits_by_month = self.data['commits'].groupby('month')['commits'].sum()
        sns.lineplot(x=commits_by_month.index, y=commits_by_month.values, 
                marker='o', label='Commits')

        # Plot PRs
        prs_by_month = self.data['pull_requests'].groupby('month')['count'].sum()
        sns.lineplot(x=prs_by_month.index, y=prs_by_month.values, 
                marker='s', label='Pull Requests')

        # Plot file changes
        changes = self.data['file_changes']
        total_changes = changes.groupby('month')['lines_added'].sum() + \
                       changes.groupby('month')['lines_removed'].sum()
        sns.lineplot(x=total_changes.index, y=total_changes.values, 
                marker='^', label='File Changes')

        plt.title('Monthly Development Activity')
        plt.xlabel('Month')
        plt.ylabel('Count')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        
        plt.savefig(f"{self.output_folder}/monthly_activity.png")
        plt.close()
        logger.info("Generated monthly activity chart")

    def create_repository_contributions_chart(self):
        """Create bar chart for top repositories by commit count."""
        commits_by_repo = self.data['commits'].groupby('repository')['commits'].sum()
        commits_by_repo = commits_by_repo.sort_values(ascending=False).head(10)

        plt.figure(figsize=(12, 6))
        sns.barplot(x=commits_by_repo.index, y=commits_by_repo.values)
        plt.title('Top 10 Repositories by Commit Count')
        plt.xlabel('Repository')
        plt.ylabel('Number of Commits')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        plt.savefig(f"{self.output_folder}/repository_contributions.png")
        plt.close()
        logger.info("Generated repository contributions chart")

    def create_file_changes_summary(self):
        """Create stacked bar chart for lines added vs removed per month."""
        changes = self.data['file_changes']
        monthly_changes = changes.groupby('month').agg({
            'lines_added': 'sum',
            'lines_removed': 'sum'
        })

        plt.figure(figsize=(12, 6))
        sns.barplot(x=monthly_changes.index, y=monthly_changes['lines_added'], label='Lines Added')
        sns.barplot(x=monthly_changes.index, y=monthly_changes['lines_removed'], bottom=monthly_changes['lines_added'], label='Lines Removed')
        plt.title('File Changes by Month')
        plt.xlabel('Month')
        plt.ylabel('Number of Lines')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        plt.savefig(f"{self.output_folder}/file_changes.png")
        plt.close()
        logger.info("Generated file changes summary chart")

    def create_contribution_distribution_chart(self):
        """Create pie chart showing contribution distribution by repository."""
        commits_by_repo = self.data['commits'].groupby('repository')['commits'].sum()
        commits_by_repo = commits_by_repo.sort_values(ascending=False)
        
        # Take top 5 repos and group others
        top_repos = commits_by_repo.head(5)
        others = pd.Series({'Others': commits_by_repo[5:].sum()})
        plot_data = pd.concat([top_repos, others])

        plt.figure(figsize=(10, 10))
        plt.pie(plot_data.values, labels=plot_data.index, autopct='%1.1f%%')
        plt.title('Repository Contribution Distribution')
        plt.axis('equal')
        
        plt.savefig(f"{self.output_folder}/contribution_distribution.png")
        plt.close()
        logger.info("Generated contribution distribution chart")

    def generate_all_visualizations(self):
        """Generate all visualization charts."""
        self.create_monthly_activity_chart()
        self.create_repository_contributions_chart()
        self.create_file_changes_summary()
        self.create_contribution_distribution_chart()
