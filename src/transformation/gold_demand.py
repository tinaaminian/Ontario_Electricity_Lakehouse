from pyspark.sql import DataFrame
from pyspark.sql import functions as F
def transform_daily_demand(silver_df:DataFrame) -> DataFrame:
    """
        Aggregate hourly demand into daily metrics
    """
    return (
        silver_df.groupBy("demand_date")
        .agg(
            F.avg("ontario_demand_raw").alias("avg_demand_mw"),
            F.max("ontario_demand_raw").alias("peak_demand_mw"),
            F.min("ontario_demand_raw").alias("min_demand_mw"),
            F.count("*").alias("observed_hours")
        ).withColumn(
            "is_complete",F.col("observed_hours") == 24
        )
    )
