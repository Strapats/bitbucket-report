import os
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from bitbucket_api import BitbucketAPI
from data_aggregator import DataAggregator
from visualizer import Visualizer

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def load_existing_data(output_dir: str) -> dict:
    """Load existing CSV files from the output directory."""
    data = {}
    output_path = Path(output_dir)
    
    # Expected CSV files
    commits_file = output_path / 'commits.csv'
    diffstats_file = output_path / 'diffstats.csv'
    
    if not commits_file.exists():
        raise FileNotFoundError(
            f"Could not find commits.csv in {output_dir}. "
            "Please run data collection first or specify a different output directory."
        )
    
    if not diffstats_file.exists():
        raise FileNotFoundError(
            f"Could not find diffstats.csv in {output_dir}. "
            "Please run data collection first or specify a different output directory."
        )
    
    try:
        import pandas as pd
        data['commits'] = pd.read_csv(commits_file, parse_dates=['date'])
        data['diffstats'] = pd.read_csv(diffstats_file)
        logging.info(f"Successfully loaded data from {output_dir}")
        logging.info(f"Found {len(data['commits'])} commits and {len(data['diffstats'])} diffstats")
        return data
    except Exception as e:
        raise Exception(f"Error loading CSV files: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Bitbucket Statistics Generator')
    parser.add_argument('--year', type=int, default=datetime.now().year,
                      help='Year to collect statistics for (default: current year)')
    parser.add_argument('--visualize-only', action='store_true',
                      help='Only generate visualizations from existing CSV files')
    parser.add_argument('--output-dir', type=str, default='output',
                      help='Output directory for data and visualizations')
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        if args.visualize_only:
            logging.info(f"Loading existing data from {args.output_dir} for visualization...")
            try:
                data = load_existing_data(args.output_dir)
            except FileNotFoundError as e:
                logging.error(str(e))
                logging.error(f"To collect new data, run without --visualize-only flag:")
                logging.error(f"./run.sh --year {args.year} --output-dir {args.output_dir}")
                return 1
            
            visualizer = Visualizer(output_dir)
            visualizer.generate_visualizations(data)
            logging.info(f"✨ Visualizations generated successfully in {output_dir}")
        else:
            logging.info(f"Starting data collection for year {args.year}")
            api = BitbucketAPI()
            aggregator = DataAggregator(api)
            aggregator.year = args.year
            visualizer = Visualizer(output_dir)
            
            data = aggregator.collect_data()
            visualizer.generate_visualizations(data)
            logging.info(f"✨ Data collection and visualization completed successfully")
        
        return 0
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
