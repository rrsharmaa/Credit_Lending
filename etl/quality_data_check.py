from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim

class QualityDataCheck:
    """Class to perform data quality transformations on a DataFrame."""

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe

    def trim_columns(self, columns: list) -> "QualityDataCheck":
        """Trim leading and trailing spaces in specified columns."""
        for column in columns:
            self.dataframe = self.dataframe.withColumn(column, trim(col(column)))
        return self

    def remove_duplicates(self, key_columns: list) -> "QualityDataCheck":
        """Remove duplicate rows based on specified key columns."""
        self.dataframe = self.dataframe.dropDuplicates(key_columns)
        return self

    def apply_transformation(self, column: str, transformation) -> "QualityDataCheck":
        """Apply a custom transformation function to a specified column."""
        self.dataframe = self.dataframe.withColumn(column, transformation(col(column)))
        return self

    def get_dataframe(self) -> DataFrame:
        """Return the transformed DataFrame."""
        return self.dataframe
