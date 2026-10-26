from multiprocessing import Value
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_daily_demand(gold_df:DataFrame) -> None:
    """
        Validate the gold daily demand dataset.
    """

    # Validate that dataframe is not empty
    if gold_df.limit(1).count() == 0:
        raise ValueError("Gold validation failed, dataframe is empty")
    
    # Validate no nulls
    if gold_df.filter(F.col("demand_date").isNull()).count() > 0:
            raise ValueError("gold validation failed: null dates")
    

    # Validate unique dates
    duplicate_dates = (
        gold_df.groupBy("demand_date").count().filter(F.col("count")> 1)
    )
    if duplicate_dates.count() > 0:
        raise ValueError("gold validation failed: duplicate dates")
    
    # Validate observed_hours between 1 and 24
    invalid_hours = gold_df.filter(~F.col("observed_hours").between(1,24) | F.col("observed_hours").isNull())
    
    if invalid_hours.count() > 0:
        raise ValueError("Gold validation failed: Invalid observed_hours")

    # Validate demand metrics are positive and on-null
    demand_columns = [
        "avg_demand_mw",
        "min_demand_mw",
        "peak_demand_mw"
    ]
    for col in demand_columns:
        invalid_demand = gold_df.filter(
            F.col(col).isNull()|
            (F.col(col) < 0) 
        )
        
        if invalid_demand.count() > 0:
            raise ValueError(f"Gold validation failed: Invalid {col}")
    
    print("Pass, gold deaily demand validation completed")