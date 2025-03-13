from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from logger import setup_logger 
from constants import LOG_FILE
# Set up logger
logger = setup_logger(LOG_FILE)

def read_csv(spark: SparkSession, filepath: str) -> DataFrame:
    try:
        logger.info(f"Reading CSV file: {filepath}")
        df_csv = spark.read.csv(filepath, header=True, inferSchema=True)
        logger.info("CSV file read successfully")
        return df_csv
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise e

def read_json(spark: SparkSession, filepath: str) -> DataFrame:
    try:
        # Defining the schema for the inner "stocks" array
        stocks_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        # Define the schema for the entire file
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("stocks", ArrayType(stocks_schema), True)
        ])

        logger.info(f"Reading JSON file: {filepath}")
        df_json = spark.read.schema(schema).json(filepath, multiLine=True)
        logger.info("JSON file read successfully")
        return df_json
    except Exception as e:
        logger.error(f"Error reading JSON file: {str(e)}")
        raise e
