import pandas as pd
from typing import Dict
import os
from datetime import datetime
import logging
from weasyprint import HTML
from jinja2 import Template

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, data: Dict[str, pd.DataFrame], output_folder: str, year: int):
        self.data = data
        self.output_folder = output_folder
        self.year = year

    def generate_html_report(self) -> str:
        """Generate HTML report with embedded visualizations."""
        template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Year-End Development Report {{ year }}</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                h1 { color: #333; }
                .section { margin: 20px 0; }
                .chart { margin: 20px 0; text-align: center; }
                img { max-width: 100%; }
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f5f5f5; }
            </style>
        </head>
        <body>
            <h1>Year-End Development Report {{ year }}</h1>
            
            <div class="section">
                <h2>Overview</h2>
                <p>Total Repositories: {{ total_repos }}</p>
                <p>Total Commits: {{ total_commits }}</p>
                <p>Total Pull Requests: {{ total_prs }}</p>
                <p>Total Lines Added: {{ total_lines_added }}</p>
                <p>Total Lines Removed: {{ total_lines_removed }}</p>
            </div>

            <div class="section">
                <h2>Monthly Activity</h2>
                <div class="chart">
                    <img src="monthly_activity.png" alt="Monthly Activity Chart">
                </div>
            </div>

            <div class="section">
                <h2>Repository Contributions</h2>
                <div class="chart">
                    <img src="repository_contributions.png" alt="Repository Contributions">
                </div>
            </div>

            <div class="section">
                <h2>File Changes</h2>
                <div class="chart">
                    <img src="file_changes.png" alt="File Changes Summary">
                </div>
            </div>

            <div class="section">
                <h2>Contribution Distribution</h2>
                <div class="chart">
                    <img src="contribution_distribution.png" alt="Contribution Distribution">
                </div>
            </div>

            <div class="section">
                <h2>Detailed Statistics</h2>
                {{ detailed_stats | safe }}
            </div>
        </body>
        </html>
        """)

        # Calculate statistics
        total_repos = len(self.data['commits']['repository'].unique())
        total_commits = self.data['commits']['commits'].sum()
        total_prs = self.data['pull_requests']['count'].sum()
        total_lines_added = self.data['file_changes']['lines_added'].sum()
        total_lines_removed = self.data['file_changes']['lines_removed'].sum()

        # Generate detailed statistics table
        detailed_stats = self.data['commits'].merge(
            self.data['pull_requests'].groupby(['repository', 'month'])['count'].sum().reset_index(),
            on=['repository', 'month'],
            how='outer'
        ).fillna(0)
        
        detailed_stats = detailed_stats.merge(
            self.data['file_changes'],
            on=['repository', 'month'],
            how='outer'
        ).fillna(0)

        # Convert detailed stats to HTML table
        detailed_stats_html = detailed_stats.to_html(
            classes='table',
            float_format=lambda x: '{:,.0f}'.format(x)
        )

        # Render HTML
        html_content = template.render(
            year=self.year,
            total_repos=total_repos,
            total_commits=total_commits,
            total_prs=total_prs,
            total_lines_added=total_lines_added,
            total_lines_removed=total_lines_removed,
            detailed_stats=detailed_stats_html
        )

        # Save HTML report
        html_path = os.path.join(self.output_folder, 'report.html')
        with open(html_path, 'w') as f:
            f.write(html_content)
        logger.info(f"Generated HTML report at {html_path}")

        return html_path

    def generate_pdf_report(self):
        """Generate PDF report from HTML."""
        html_path = self.generate_html_report()
        pdf_path = os.path.join(self.output_folder, 'report.pdf')
        
        # Convert HTML to PDF
        HTML(filename=html_path).write_pdf(pdf_path)
        logger.info(f"Generated PDF report at {pdf_path}")

    def generate_report(self):
        """Generate both HTML and PDF reports."""
        self.generate_pdf_report()
