from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, sum as sum_
from logger import setup_logger
from constants import LOG_FILE

# Initialize logger
log = setup_logger(LOG_FILE)

def evaluate_collateral_value(clients_df: DataFrame, collaterals_df: DataFrame, stocks_df: DataFrame) -> DataFrame:
    """Calculates the daily collateral value for each client based on their stock holdings."""
    try:
        log.info("Calculating collateral values...")

        # Transform the collaterals dataframe
        collaterals_df = (collaterals_df
                          .withColumn("Symbol", split(col("Stocks"), ", ").getItem(0))
                          .withColumn("Shares", split(col("Stocks"), ", ").getItem(1))
                          .withColumn("Shares", split(col("Shares"), ":").getItem(1).cast("int"))
                          .drop("Stocks"))

        # Explode the stocks array to get individual stock prices
        stocks_df = stocks_df.select(
            col("date"), explode(col("stocks")).alias("stock")
        ).select(
            col("date"), col("stock.symbol").alias("Symbol"), col("stock.price").alias("Price")
        )

        # Compute daily collateral values
        daily_collateral_df = (collaterals_df
                               .join(stocks_df, "Symbol", "inner")
                               .withColumn("daily_value", col("Shares") * col("Price"))
                               .select("Account_ID", "date", "daily_value"))

        # Map collateral values to clients
        result_df = (daily_collateral_df
                     .join(clients_df, "Account_ID", "inner")
                     .groupBy("Client_ID").pivot("date").agg(sum_("daily_value"))
                     .na.fill(0))

        log.info("Collateral values computed successfully.")
        result_df.show()

        return result_df

    except Exception as err:
        log.error(f"Error computing collateral values: {err}")
        raise
