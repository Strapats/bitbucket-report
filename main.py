import os
import logging
from bitbucket_api import BitbucketAPI
from data_aggregator import DataAggregator
from visualizer import Visualizer
from report_generator import ReportGenerator
import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Validate environment variables
    required_vars = ['BITBUCKET_WORKSPACE', 'BITBUCKET_USERNAME', 'BITBUCKET_APP_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set them in a .env file or environment")
        return

    try:
        # Initialize API client
        api = BitbucketAPI()
        
        # Initialize data aggregator and collect data
        aggregator = DataAggregator(api, config.REPORT_YEAR)
        logger.info("Starting data collection...")
        aggregator.collect_data()
        
        # Aggregate data
        logger.info("Aggregating collected data...")
        aggregated_data = aggregator.aggregate_data()
        
        # Export raw data to CSV
        logger.info("Exporting data to CSV...")
        aggregator.export_csv(aggregated_data, config.OUTPUT_FOLDER)
        
        # Generate visualizations
        logger.info("Generating visualizations...")
        visualizer = Visualizer(aggregated_data, config.OUTPUT_FOLDER)
        visualizer.generate_all_visualizations()
        
        # Generate report
        logger.info("Generating final report...")
        report_generator = ReportGenerator(aggregated_data, config.OUTPUT_FOLDER, config.REPORT_YEAR)
        report_generator.generate_report()
        
        logger.info("Report generation completed successfully!")
        logger.info(f"Output files are available in: {config.OUTPUT_FOLDER}")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return

if __name__ == "__main__":
    main()
