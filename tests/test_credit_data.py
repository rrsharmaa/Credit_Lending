import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

from src.transformation import evaluate_collateral_value


class TestCreditLending(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test") \
            .master("local[0]") \
            .getOrCreate()

    def test_calculate_collateral_value(self):
        # Create input data
        collaterals_data = [("1", "ABCD:10, XYZA:21"), ("2", "JKLM:12, ABCD:23")]
        collaterals_df = self.spark.createDataFrame(collaterals_data, ["Account_ID", "Stocks"])

        clients_data = [("1", "Client1"), ("2", "Client2")]
        clients_df = self.spark.createDataFrame(clients_data, ["Account_ID", "Client_ID"])

        stocks_data = [("2025-03-14", [{"symbol": "ABCD", "price": 190.5},
                                       {"symbol": "XYZA", "price": 210.65},
                                       {"symbol": "JKLM", "price": 278.50}])]
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
        result_df = evaluate_collateral_value(clients_df, collaterals_df, stocks_df)

        # Define expected schema and data
        expected_data = [
            ("1", 3456.75, 0, 0, 0, 0),  # Collateral values for Client 1 over 5 days
        ]
        expected_schema = StructType([
            StructField("Client_ID", StringType(), True),
            StructField("2025-03-14", DoubleType(), False),
            StructField("2025-03-15", DoubleType(), False),
            StructField("2025-03-16", DoubleType(), False),
            StructField("2025-03-17", DoubleType(), False),
            StructField("2025-03-18", DoubleType(), False)
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
