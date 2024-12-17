import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict, Union
from pathlib import Path
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Visualizer:
    """Class to generate visualizations from collected data."""
    
    def __init__(self, output_dir: Union[str, Path]):
        """Initialize visualizer with output directory."""
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger(__name__)
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set style for all plots
        plt.style.use('seaborn-v0_8-darkgrid')  # Modern style that's available in matplotlib
        
        # Set default figure size and DPI for better quality
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        
        # Set color scheme
        plt.rcParams['axes.prop_cycle'] = plt.cycler('color', ['#2ecc71', '#e74c3c', '#3498db', '#f1c40f', '#9b59b6'])
        
    def generate_visualizations(self, data: Dict[str, pd.DataFrame]):
        """Generate all visualizations from the data."""
        commits_df = data['commits']
        diffstats_df = data['diffstats']
        
        # Save DataFrames to CSV
        self.logger.info("Saving data to CSV files...")
        commits_df.to_csv(self.output_dir / 'commits.csv', index=False)
        diffstats_df.to_csv(self.output_dir / 'diffstats.csv', index=False)
        
        # Generate visualizations
        self.logger.info("Generating visualizations...")
        
        # Monthly commit activity
        self.plot_monthly_commits(commits_df)
        
        # Repository activity
        self.plot_repository_activity(commits_df)
        
        # Code changes
        self.plot_code_changes(diffstats_df)
        
        self.logger.info(f"✨ All visualizations saved to {self.output_dir}")
    
    def plot_monthly_commits(self, commits_df: pd.DataFrame):
        """Plot monthly commit activity."""
        plt.figure(figsize=(12, 6))
        
        monthly_commits = commits_df.groupby('month').size()
        monthly_commits.plot(kind='bar')
        
        plt.title('Monthly Commit Activity')
        plt.xlabel('Month')
        plt.ylabel('Number of Commits')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'monthly_commits.png')
        plt.close()
        
        self.logger.info("📊 Generated monthly commit activity plot")
    
    def plot_repository_activity(self, commits_df: pd.DataFrame):
        """Plot repository commit activity."""
        plt.figure(figsize=(12, 6))
        
        repo_commits = commits_df.groupby('repository').size().sort_values(ascending=True)
        repo_commits.plot(kind='barh')
        
        plt.title('Repository Activity')
        plt.xlabel('Number of Commits')
        plt.ylabel('Repository')
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'repository_activity.png')
        plt.close()
        
        self.logger.info("📊 Generated repository activity plot")
    
    def plot_code_changes(self, diffstats_df: pd.DataFrame):
        """Plot code changes (lines added/removed)."""
        plt.figure(figsize=(12, 6))
        
        total_changes = pd.DataFrame({
            'Lines Added': [diffstats_df['lines_added'].sum()],
            'Lines Removed': [diffstats_df['lines_removed'].sum()]
        })
        
        total_changes.plot(kind='bar')
        plt.title('Total Code Changes')
        plt.xlabel('Type of Change')
        plt.ylabel('Number of Lines')
        plt.xticks(rotation=0)
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'code_changes.png')
        plt.close()
        
        self.logger.info("📊 Generated code changes plot")
