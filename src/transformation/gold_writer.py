from pyspark.sql import DataFrame


def write_gold_daily_demand(gold_df:DataFrame, target_table:str) -> None:
    """
        Persist validated gold faily demand data as a delta table.
        Strategy: full-refresh overwrite.
    """
    if gold_df.isStreaming:
        raise ValueError("gold writer requires batch dataframe")
    
    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)

    )
    