from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_silver_demand(bronze_df:DataFrame) -> DataFrame:
    """
        Converts validated bronze data to silver
    """
    return(
        bronze_df.select(
            F.to_date("date_raw", "yyyy-MM-dd").alias("demand_date"),
            F.expr("try_cast(hour_raw as INT)").alias("hour_ending"),
            F.expr("try_cast(market_demand_raw as DECIMAL(12,2))").alias("market_demand_mw"),
            F.expr("try_cast(ontario_demand_raw as DECIMAL(12,2))").alias("ontario_demand_raw"),
            F.col("source_file_name"),
            F.col("ingested_at")
        ).withColumn("processed_at", F.current_timestamp())
    )
