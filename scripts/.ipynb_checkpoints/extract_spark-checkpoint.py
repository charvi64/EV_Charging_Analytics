# =============================================================
# EXTRACT MODULE (Apache Spark)
# =============================================================

from pyspark.sql import SparkSession

def extract_data(file_path: str):
    """
    Extract raw EV charging data from a CSV file using Spark.
    """
    print("🚀 [EXTRACT] Starting data extraction...")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("EV_Charging_Extract") \
        .getOrCreate()

    # Read CSV
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print(f"✅ [EXTRACT] Loaded {df.count()} rows and {len(df.columns)} columns.")
    
    return df
