# =============================================================
# LOAD MODULE (Apache Spark)
# =============================================================

from pyspark.sql import DataFrame
import os

def load_data(df, target_path):
    print("💾 [LOAD] Saving cleaned data...")

    # Save cleaned DataFrame as Parquet
    df.write.mode("overwrite").parquet(target_path)

    print(f"✅ [LOAD] Data successfully saved to: {target_path}")

