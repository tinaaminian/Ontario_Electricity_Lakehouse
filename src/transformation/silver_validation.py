from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_silver_demand(df:DataFrame) -> None:
    """
        Validate Silver data before writing.
    """
    invalide_rows = df.filter(
    F.col("demand_date").isNull()|
    F.col("hour_ending").isNull()|
    F.col("market_demand_mw").isNull()|
    F.col("ontario_demand_raw").isNull()|
    ~F.col("hour_ending").between(1,24)|
    (F.col("market_demand_raw") <=0)|
    (F.col("ontario_demand_raw") <=0)
)
    invalid_count = invalide_rows.count()

    if invalid_count > 0:
        raise ValueError(f"silver validation failed {invalid_count} invalid rows")
    
    duplicate_keys = (
        df.groupBy("demand_date", "hour_ending").count().filter(F.col("count") > 1)
    )
    if duplicate_keys.count() > 0:
        raise ValueError("silver validation failed due to duplicate keys")