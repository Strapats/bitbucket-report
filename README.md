# Bitbucket Year-End Report Generator

This project generates a comprehensive year-end report of development activity across all repositories in a Bitbucket Cloud workspace. The report includes statistics and visualizations for commits, pull requests, and file changes.

## Features

- Fetches data from Bitbucket Cloud API:
  - Repository information
  - Commits
  - Pull requests
  - File changes (lines added/removed)
- Generates visualizations:
  - Monthly activity trends
  - Repository contributions
  - File changes summary
  - Contribution distribution
- Exports data in multiple formats:
  - CSV files for raw data
  - HTML report with interactive visualizations
  - PDF report for easy sharing

## Prerequisites

- Python 3.8 or higher
- Bitbucket Cloud account with API access
- App password with repository read permissions

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your Bitbucket credentials:
```env
BITBUCKET_WORKSPACE=your_workspace
BITBUCKET_USERNAME=your_username
BITBUCKET_APP_PASSWORD=your_app_password
```

## Usage

Run the script:
```bash
python main.py
```

The script will:
1. Fetch all repository data from your Bitbucket workspace
2. Process and aggregate the data
3. Generate visualizations
4. Create HTML and PDF reports
5. Save all output files to the `output` directory

## Output Files

The script generates the following files in the `output` directory:

- `commits_data.csv`: Monthly commit statistics per repository
- `pull_requests_data.csv`: Pull request data
- `file_changes_data.csv`: Lines added/removed statistics
- `monthly_activity.png`: Line chart of monthly activity
- `repository_contributions.png`: Bar chart of top repositories
- `file_changes.png`: Stacked bar chart of file changes
- `contribution_distribution.png`: Pie chart of repository contributions
- `report.html`: Complete HTML report with all visualizations
- `report.pdf`: PDF version of the report

## Configuration

You can modify the following settings in `config.py`:
- Report year (defaults to current year)
- Output folder location
- API endpoints and parameters

## Error Handling

The script includes comprehensive error handling and logging:
- Validates required environment variables
- Handles API rate limits and errors
- Logs all operations with appropriate detail levels
- Provides clear error messages for troubleshooting

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
