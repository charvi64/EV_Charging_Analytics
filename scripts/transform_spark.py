# =============================================================
# TRANSFORM MODULE (Apache Spark)
# =============================================================

from pyspark.sql.functions import col, mean

def transform_data(df):
    print("🧹 [TRANSFORM] Starting data cleaning...")

    # Remove duplicates
    df = df.dropDuplicates()

    # Example: fill missing numeric values
    if "charging_cost" in df.columns:
        avg_cost = df.select(mean(col("charging_cost"))).collect()[0][0]
        df = df.fillna({"charging_cost": avg_cost})

    # Example: handle missing ratings
    if "reviews_rating" in df.columns:
        avg_rating = df.select(mean(col("reviews_rating"))).collect()[0][0]
        df = df.fillna({"reviews_rating": avg_rating})

    print("✅ [TRANSFORM] Data cleaning completed successfully.")
    return df
