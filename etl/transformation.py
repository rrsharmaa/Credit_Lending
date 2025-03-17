from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split
from pyspark.sql import functions as F
from logger import setup_logger  # Ensure you have the logger setup module ready
from constants import LOG_FILE
# Set up the logger
logger = setup_logger(LOG_FILE)


def evaluate_collateral_value(clients_df: DataFrame, collaterals_df: DataFrame, stocks_df: DataFrame) -> DataFrame:
    try:
        logger.info("Starting to calculate collateral values.")

        # Flatten the Stocks column into individual rows
        collaterals_transformed_df = (collaterals_df
                                      .withColumn("Stock", explode(split(col("Stocks"), ", ")))
                                      .withColumn("Symbol", split(col("Stock"), ":").getItem(0))
                                      .withColumn("number_of_share", split(col("Stock"), ":").getItem(1).cast("int"))
                                      .drop("Stock", "Stocks"))

        logger.debug("Collaterals transformed dataframe:\n{}".format(collaterals_transformed_df))

        # Explode the stocks array
        stocks_exploded = stocks_df.select(
            col("date"),
            explode(col("stocks")).alias("stock")
        ).select(
            col("date"),
            col("stock.symbol").alias("Symbol"),
            col("stock.price").alias("Price")
        )

        logger.debug("Stocks exploded dataframe:\n{}".format(stocks_exploded))

        # Join collaterals_df with stocks_exploded to calculate daily collateral values
        daily_collateral_values = collaterals_transformed_df.join(
            stocks_exploded,
            collaterals_transformed_df.Symbol == stocks_exploded.Symbol,
            'inner'
        ).withColumn(
            "daily_value",
            F.col("number_of_share") * F.col("Price")
        ).select("Account_ID", "date", "daily_value")

        logger.debug("Daily collateral values:\n{}".format(daily_collateral_values))

        # Join with clients and pivot the DataFrame to get the daily totals per client
        daily_collateral_values_with_client = daily_collateral_values.join(
            clients_df,
            daily_collateral_values.Account_ID == clients_df.Account_ID,
            "inner"
        ).select(
            clients_df.Client_ID,
            "date",
            "daily_value"
        )

        client_daily_totals = daily_collateral_values_with_client \
            .groupBy("Client_ID", "Date") \
            .agg(F.sum("Daily_Value").alias("Daily_value")) \
            .orderBy("Client_ID", "date") \
            .na.fill(0)

        logger.info("Successfully calculated the daily totals per client.")

        return client_daily_totals
    except Exception as e:
        logger.error(f"Error in calculating collateral values: {e}")
        raise
