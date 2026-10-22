from pyspark.sql import DataFrame, SparkSession
from ingestion.config import  SourceConfig
from ingestion.schema import IESO_DEMAND_2025
from pathlib import Path

def read_stream(spark, config: SourceConfig) -> DataFrame:
    """
        Create an auto loader streaming Dataframe for source csv files.
        Reads files incrementally from the source directory.
    """
    df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", config.file_format)
      .option("header", "true")
      .option("comment", "\\")
      .option("cloudFiles.includeExistingFiles", "true")
      .option("mode", "FAILFAST")
      .schema(IESO_DEMAND_2025)
      .load(str(Path(config.file_path).parent))
    )
    return df