from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, split, trim, when, size

def run_analytics():
    spark = SparkSession.builder.appName("EV_Charging_Analytics").getOrCreate()

    print("\n==========================================")
    print("🚀 STARTING ANALYTICS ON CLEANED EV DATA")
    print("==========================================")

    # Load cleaned data
    cleaned_path = "/Users/charvisaig/ev_charging_analysis/output/cleaned_ev_data_spark"
    df = spark.read.parquet(cleaned_path)

    print(f"✅ Loaded {df.count()} rows and {len(df.columns)} columns for analysis.")

    # Split Address into array
    address_parts = split(col("Address"), ",")

    # Compute safe indices
    df = df.withColumn("address_size", size(address_parts)) \
           .withColumn("state",
                       when(col("address_size") >= 1, trim(address_parts.getItem(col("address_size") - 1)))
                       .otherwise(None)) \
           .withColumn("city",
                       when(col("address_size") >= 2, trim(address_parts.getItem(col("address_size") - 2)))
                       .otherwise(None)) \
           .withColumnRenamed("Station Operator", "operator_name") \
           .withColumnRenamed("Cost (USD/kWh)", "charging_cost")

    df.createOrReplaceTempView("ev_stations")

    # Queries
    print("\n📊 [1] Average charging cost by city:")
    avg_cost = spark.sql("""
        SELECT city, ROUND(AVG(charging_cost), 2) AS avg_cost
        FROM ev_stations
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY avg_cost DESC
        LIMIT 10
    """)
    avg_cost.show()

    print("\n⚡ [2] Number of stations per state:")
    state_count = spark.sql("""
        SELECT state, COUNT(*) AS station_count
        FROM ev_stations
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY station_count DESC
        LIMIT 10
    """)
    state_count.show()

    print("\n🔋 [3] Top 10 operators with most chargers:")
    top_operators = spark.sql("""
        SELECT operator_name, COUNT(*) AS total_stations
        FROM ev_stations
        GROUP BY operator_name
        ORDER BY total_stations DESC
        LIMIT 10
    """)
    top_operators.show()

    # Save results
    output_path = "../output/analytics_results"
    avg_cost.write.mode("overwrite").parquet(f"{output_path}/avg_cost_by_city")
    state_count.write.mode("overwrite").parquet(f"{output_path}/station_count_by_state")
    top_operators.write.mode("overwrite").parquet(f"{output_path}/top_operators")

    print("\n✅ Analytics results saved successfully!")
    print("==========================================\n")

if __name__ == "__main__":
    run_analytics()
