from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    #started spark session
    spark = SparkSession.builder \
        .appName("UrbanMobilityScalableETL") \
        .getOrCreate()

    #read csv files using wsl path
    df = spark.read.csv(
        "/mnt/d/Blend assignments/task/cleaned_yellow_tripdata.csv",
        header=True,
        inferSchema=True
    )

    #converted timestamps
    df = df.withColumn(
        "tpep_pickup_datetime",
        F.to_timestamp("tpep_pickup_datetime")
    ).withColumn(
        "tpep_dropoff_datetime",
        F.to_timestamp("tpep_dropoff_datetime")
    )

    #filtered invalid timestamps
    df = df.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
    )

    #filtered invalid passenger counts
    df = df.filter(F.col("passenger_count") > 0)

    #filtered invalid trip distances
    df = df.filter(F.col("trip_distance") > 0)

    #added time features
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
           .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
           .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))

    #computed monthly revenue
    monthly_revenue_df = (
        df.groupBy("pickup_month")
          .agg(F.sum("total_amount").alias("monthly_revenue"))
          .orderBy("pickup_month")
    )

    #defined peak hours
    df = df.withColumn(
        "is_peak",
        F.col("pickup_hour").isin([7,8,9,16,17,18])
    )

    #computed peak congestion
    peak_congestion_df = (
        df.groupBy("is_peak")
          .agg(
              F.count("*").alias("trip_count"),
              F.avg("trip_distance").alias("avg_trip_distance")
          )
    )

    #computed revenue per mile
    df = df.withColumn(
        "revenue_per_mile",
        F.col("total_amount") / F.col("trip_distance")
    )

    #filtered high value trips
    high_value_trips_df = (
        df.filter(F.col("revenue_per_mile") > 10)
          .select(
              "pickup_hour",
              "pickup_month",
              "trip_distance",
              "total_amount",
              "revenue_per_mile"
          )
    )

    #wrote parquet outputs
    monthly_revenue_df.write.mode("overwrite").parquet(
        "/mnt/d/Blend assignments/task/parquet/monthly_revenue"
    )

    peak_congestion_df.write.mode("overwrite").parquet(
        "/mnt/d/Blend assignments/task/parquet/peak_congestion"
    )

    high_value_trips_df.write.mode("overwrite").parquet(
        "/mnt/d/Blend assignments/task/parquet/high_value_trips"
    )

    #printed execution plan
    monthly_revenue_df.explain(True)


    spark.stop()

if __name__ == "__main__":
    main()
