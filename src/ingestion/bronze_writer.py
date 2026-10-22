
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_bronze_stream(df:DataFrame, target_table: str, checkpoint_path: str) -> StreamingQuery:
    """
        weite an auto loader dataframe to a bronze delta table.
    """

    # ensure that we are receiving a streaming dataframe
    if not df.isStreaming:
        raise ValueError("Expected a streaming DataFrame")
    
    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation",checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query