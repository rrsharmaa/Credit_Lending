from pyspark.sql import DataFrame
import os


def save_as_parquet(df: DataFrame, base_output_path: str):
    # Get the base output directory
    output_directory = os.path.dirname(base_output_path)

    # Get the base filename without extension
    base_filename = os.path.splitext(os.path.basename(base_output_path))[0]

    # Initialize a counter
    counter = 1

    # Generate a new output path until a non-existing one is found
    output_path = os.path.join(output_directory, f"{base_filename}.parquet")
    while os.path.exists(output_path):
        output_path = os.path.join(output_directory, f"{base_filename}_{counter}.parquet")
        counter += 1

    # Save the DataFrame as a Parquet file
    df.write.format("parquet").save(output_path)
    print(f"DataFrame saved as: {output_path}")
