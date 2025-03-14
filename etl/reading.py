from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from logger import setup_logger
from constants import LOG_FILE

# Initialize logger
log = setup_logger(LOG_FILE)

def load_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """Loads a CSV file into a Spark DataFrame."""
    try:
        log.info(f"Attempting to load CSV data from: {file_path}")
        data_frame = spark.read.csv(file_path, header=True, inferSchema=True)
        log.info("CSV file successfully loaded into DataFrame")
        return data_frame
    except Exception as error:
        log.error(f"Failed to read CSV file: {error}")
        raise

def load_json(spark: SparkSession, file_path: str) -> DataFrame:
    """Reads a JSON file and loads it into a Spark DataFrame with a predefined schema."""
    try:
        # Schema definition for the nested "stocks" array
        stock_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        # Schema for the overall JSON structure
        json_schema = StructType([
            StructField("date", StringType(), True),
            StructField("stocks", ArrayType(stock_schema), True)
        ])

        log.info(f"Loading JSON data from: {file_path}")
        json_df = spark.read.schema(json_schema).json(file_path, multiLine=True)
        log.info("JSON file successfully processed")
        return json_df
    except Exception as error:
        log.error(f"Error while loading JSON file: {error}")
        raise
