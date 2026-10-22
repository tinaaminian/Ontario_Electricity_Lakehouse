from pyspark.sql import DataFrame, SparkSession
from src.ingestion.config import  SourceConfig
from src.ingestion.schema import IESO_DEMAND_BRONZE_SCHEMA
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
      .schema(IESO_DEMAND_BRONZE_SCHEMA)
      .load(str(Path(config.file_path).parent))
    )
    return df