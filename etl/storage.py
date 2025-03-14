from pyspark.sql import DataFrame
import os


def save_as_parquet(df: DataFrame, output_path: str):
    """Saves a DataFrame as a Parquet file, ensuring unique filenames if conflicts exist."""

    base_dir, filename = os.path.dirname(output_path), os.path.splitext(os.path.basename(output_path))[0]
    counter = 1
    final_path = os.path.join(base_dir, f"{filename}.parquet")

    while os.path.exists(final_path):
        final_path = os.path.join(base_dir, f"{filename}_{counter}.parquet")
        counter += 1

    df.write.parquet(final_path)
    print(f"DataFrame saved at: {final_path}")
