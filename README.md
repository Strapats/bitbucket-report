# Bitbucket Statistics

A tool to collect and visualize statistics from your Bitbucket repositories, including commit activity, code changes, and author contributions.

## Features

- Collects repository statistics from Bitbucket Cloud
- Tracks commit activity over time
- Analyzes code changes (lines added/removed)
- Generates author contribution statistics
- Caches API responses for faster subsequent runs
- Handles rate limiting and retries automatically

## Prerequisites

- Python 3.x
- Bitbucket Cloud account with API access
- macOS (for automatic dependency installation)

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd bitbucket-statistics
```

2. Run the setup script:
```bash
chmod +x run.sh
./run.sh
```

The script will:
- Install system dependencies (using Homebrew on macOS)
- Create a Python virtual environment
- Install required Python packages
- Create a template `.env` file if it doesn't exist

3. Configure your Bitbucket credentials:
Edit the `.env` file and fill in your Bitbucket details:
```
BITBUCKET_WORKSPACE=your-workspace-here
BITBUCKET_USERNAME=your-username-here
BITBUCKET_APP_PASSWORD=your-app-password-here
```

To create an app password:
1. Go to Bitbucket Settings → App passwords
2. Create a new app password with the following permissions:
   - Repositories: Read
   - Pull requests: Read

## Usage

Run the script:
```bash
./run.sh
```

The script will:
1. Collect statistics from all repositories in your workspace
2. Generate CSV reports in the `output` directory
3. Create visualizations of the data

Data is cached in the `cache` directory to speed up subsequent runs.

## Output

The tool generates several files in the `output` directory:
- `monthly_commits_data.csv`: Commit activity by repository and month
- `author_stats_data.csv`: Contribution statistics by author

## Troubleshooting

If you encounter rate limiting (429 errors), the tool will automatically:
1. Respect the rate limits
2. Wait for the specified retry period
3. Resume data collection where it left off

## License

This project is licensed under the MIT License - see the LICENSE file for details.
