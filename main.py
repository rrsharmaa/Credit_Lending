from pyspark.sql import SparkSession
from src.data_ingestion import read_csv, read_json
from src.datat_quality_checker import  DataQualityEnhancer
from src.data_processing import calculate_collateral_value
from src.data_storage import save_as_parquet
from logger import setup_logger  # Import the setup_logger function
import warnings
from constants import LOG_FILE
# Ignore all warnings for better clarity
warnings.simplefilter('ignore')

def main():
    # Initialize logger

    # Set up the logger
    logger = setup_logger(LOG_FILE)

    # Start Spark session
    spark = SparkSession.builder.appName("DataPipeline").getOrCreate()
    logger.info("Spark session started")

    # Paths to data
    clients_path = "data/input_data/Clients.csv"
    collaterals_path = "data/input_data/Collaterals.csv"
    stocks_path = "data/input_data/Stocks.json"
    output_path = "data/output_data/collateral_status.parquet"

    # Ingest data
    logger.info("Ingesting data")
    clients_df = read_csv(spark, clients_path)
    collaterals_df = read_csv(spark, collaterals_path)
    stocks_df = read_json(spark, stocks_path)

    #  Data Quality Check
    logger.info("Checking data quality")
    # enhancer = DataQualityEnhancer(stocks_df)
    # cleaned_stocks_df = enhancer.trim_extra_spaces(["symbol"]).remove_duplicates(["date", "symbol"]).get_dataframe()
    # canonicalize_symbol = udf(lambda x: x.upper() if x else None, StringType())
    # cleaned_stocks_df = enhancer.canonicalize_data("symbol", canonicalize_symbol).get_dataframe()
    # cleaned_stocks_df.show()

    # Apply similar steps for CSV data
    # collaterals_df = spark.read.csv("/path/to/Collaterals.csv", header=True)
    # enhancer = DataQualityEnhancer(collaterals_df)
    # cleaned_collaterals_df = enhancer.trim_extra_spaces(["Account_ID", "Savings", "Cars", "Stocks"]).remove_duplicates(
    #     ["Account_ID"]).get_dataframe()
    #
    # cleaned_collaterals_df.show()

    # Additional canonicalization and quality checks can be implemented similarly

    # Process data
    logger.info("Calculating collateral value")
    collateral_status_df = calculate_collateral_value(clients_df, collaterals_df, stocks_df)
    # df = spark.createDataFrame(collateral_status_df)

    # Save dataggi
    logger.info(f"Saving results to {output_path}")
    save_as_parquet(collateral_status_df, output_path)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
