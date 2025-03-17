import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from etl.transformation import evaluate_collateral_value

class DataProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("UnitTesting") \
            .getOrCreate()

    def test_calculate_collateral_value(self):

        # Create test input data
        clients_data = [("1", "Client1"), ("2", "Client2")]
        clients_df = self.spark.createDataFrame(clients_data, ["Account_ID", "Client_ID"])

        collaterals_data = [("1", "AAPL:10,MSFT:5"), ("2", "AAPL:15,GOOGL:8")]
        collaterals_df = self.spark.createDataFrame(collaterals_data, ["Account_ID", "Stocks"])

        stocks_data = [("2025-02-25", [{"symbol": "AAPL", "price": 150.5},
                                       {"symbol": "MSFT", "price": 250.75},
                                       {"symbol": "GOOGL", "price": 2750.0}])]
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
        expected_data = [("1", "2025-02-25", 2758.75)]
        expected_schema = StructType([
            StructField("Client_ID", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Daily_value", DoubleType(), False),
        ])
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        # Compare the data
        self.assertEqual(result_df.collect(), expected_df.collect())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


# Execute the test case
if __name__ == '__main__':
    unittest.main()
