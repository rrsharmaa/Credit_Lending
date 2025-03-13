import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lit, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType


class DataProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("UnitTesting") \
            .getOrCreate()

    def test_calculate_collateral_value(self):
        # Create test input data
        clients_data = [("1", "Client1"), ("2", "Client2")]
        clients_df = self.spark.createDataFrame(clients_data, ["Account_ID", "Client_ID"])

        collaterals_data = [("1", "AAPL:10,MSFT:5"), ("2", "AAPL:15,GOOGL:8")]
        collaterals_df = self.spark.createDataFrame(collaterals_data, ["Account_ID", "Stocks"])

        stocks_data = [("2024-02-25", [{"symbol": "AAPL", "price": 150.5},
                                       {"symbol": "MSFT", "price": 250.75},
                                       {"symbol": "GOOGL", "price": 2750}])]
        stocks_schema = StructType([
            StructField("date", StringType(), True),
            StructField("stocks", ArrayType(
                StructType([
                    StructField("symbol", StringType(), True),
                    StructField("price", DoubleType(), True)
                ])
            ), True)
        ])
        stocks_df = self.spark.createDataFrame(stocks_data, stocks_schema)

        # Assume calculate_collateral_value function exists and is imported correctly
        result_df = calculate_collateral_value(clients_df, collaterals_df, stocks_df)

        # Define expected schema and data
        expected_data = [
            ("1", 2758.75, 0, 0, 0, 0),  # Collateral values for Client 1 over 5 days
        ]
        expected_schema = StructType([
            StructField("Client_ID", StringType(), True),
            StructField("2024-02-25", DoubleType(), False),
            StructField("2024-02-26", DoubleType(), False),
            StructField("2024-02-27", DoubleType(), False),
            StructField("2024-02-28", DoubleType(), False),
            StructField("2024-02-29", DoubleType(), False)
        ])
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        # Sort both DataFrames by Account_ID to ensure they are in the same order
        result_df = result_df.orderBy("Account_ID")
        expected_df = expected_df.orderBy("Account_ID")

        # Compare the collected results
        self.assertEqual(result_df.collect(), expected_df.collect())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


# Execute the test case
if __name__ == '__main__':
    unittest.main()
