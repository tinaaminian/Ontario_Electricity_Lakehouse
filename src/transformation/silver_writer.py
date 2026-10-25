from pyspark.sql import DataFrame
def write_silver( df:DataFrame, target_table:str) -> None :
    """
        writes a validated silver dataframe into a delta table.
        This implementation uses a full-refreshed semantics.
    """
    if df.isStreaming:
        raise ValueError("Silver full-refresh writer requires a batch DataFrame")
    
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
