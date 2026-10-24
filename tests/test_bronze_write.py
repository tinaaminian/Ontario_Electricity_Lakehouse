import pytest
from src.ingestion.bronze_writer import write_bronze_stream

def test_write_bronze_stream_rejects_batch_dataframes(spark):
    """
        Verify that the streaming writer rejects batch DataFrames
        before attemting to write data.
    """

    # Arrange: Create a regulat batch DataFrame.
    df = spark.createDataFrame(
        [("2025-01-01", "1")],
        ["date_raw", "hour_raw"]
    )
    target_table = "test_bronze"
    checkpoint_path = "tmp/test_checkpoint"

    # Act + ASSERT: A batch dataframe must raise ValueError.
    with pytest.raises(Exception):
        write_bronze_stream(df,target_table,checkpoint_path)
