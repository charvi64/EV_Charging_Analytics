import pandas as pd
import pyarrow.parquet as pq

# Read all Parquet files in the folder
df = pd.read_parquet("output/cleaned_ev_data_spark")  # folder path, not a single file

# Print all column names
print(df.columns.tolist())
