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

        logger.debug("Collaterals transformed dataframe:\n{}".format(collaterals_transformed_df.show()))

        # Explode the stocks array
        stocks_exploded = stocks_df.select(
            col("date"),
            explode(col("stocks")).alias("stock")
        ).select(
            col("date"),
            col("stock.symbol").alias("Symbol"),
            col("stock.price").alias("Price")
        )

        logger.debug("Stocks exploded dataframe:\n{}".format(stocks_exploded.show()))

        # Join collaterals_df with stocks_exploded to calculate daily collateral values
        collateral_valuation_daily = collaterals_transformed_df.join(
            stocks_exploded,
            collaterals_transformed_df.Symbol == stocks_exploded.Symbol,
            'inner'
        ).withColumn(
            "daily_value",
            F.col("number_of_share") * F.col("Price")
        ).select("Account_ID", "date", "daily_value")

        logger.debug("Daily collateral values:\n{}".format(collateral_valuation_daily.show()))

        # Join with clients and pivot the DataFrame to get the daily totals per client
        collateral_valuation_daily_with_client = collateral_valuation_daily.join(
            clients_df,
            collateral_valuation_daily.Account_ID == clients_df.Account_ID,
            "inner"
        ).select(
            clients_df.Client_ID,
            "date",
            "daily_value"
        )

        client_totals_per_day = collateral_valuation_daily_with_client.groupBy("Client_ID").pivot("date").sum(
            "daily_value").na.fill(0)

        logger.info("Successfully calculated the daily totals per client.")
        print('Target_Table : Collateral_status')
        client_totals_per_day.show()

        return client_totals_per_day
    except Exception as err:
        logger.error(f"Error found in calculating collateral values: {err}")
        raise
