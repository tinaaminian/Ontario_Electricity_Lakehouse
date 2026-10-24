from src.ingestion.config import SourceConfig
from src.ingestion.schema import IESO_DEMAND_BRONZE_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from uuid import uuid4
from pathlib import Path

# reading the source file
def read_source(spark,config: SourceConfig) -> DataFrame:
    df = (
        spark.read.format(config.file_format)
        .schema(IESO_DEMAND_BRONZE_SCHEMA)
        .option("comment","\\")
        .option("header","true")
        .option("enforceSchema", "false")
        .option("mode", "failfast")
        .load(config.file_path)

    )
    return df

# preparing bronze data
def prepare_bronze(df:DataFrame,
                    config: SourceConfig,
                    batch_id: str
                    ) -> DataFrame:
    """
        Rename raw columns and add bronze ingestion metadata
        including: batch_id,source_file_path and source_file_name and ingested_timestamp 
    """

    bronze_df = (
        df.withColumnsRenamed({
            "Date": "date_raw",
            "Hour": "hour_raw",
            "Market Demand": "market_demand_raw",
            "Ontario Demand": "ontario_demand_raw"
        })
        .withColumn("source_file_path",F.lit(config.file_path))
        .withColumn("source_file_name",F.lit(Path(config.file_path).name))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return bronze_df

# creating streaming dataframe

def read_stream(spark, config: SourceConfig) -> DataFrame:
    """
        Create an auto loader streaming Dataframe for source csv files.
        Reads files incrementally from the source directory.
    """
    df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", config.file_format)
      .option("header", "true")
      .option("enforceSchema", "false")
      .option("comment", "\\")
      .option("cloudFiles.includeExistingFiles", "true")
      .option("mode", "FAILFAST")
      .schema(IESO_DEMAND_BRONZE_SCHEMA)
      .load(str(Path(config.file_path).parent))
    )
    return df

def prepare_bronze_stream(df: DataFrame) -> DataFrame:

    bronze_df_stream = (
        df.withColumnsRenamed({
            "Date": "date_raw",
            "Hour": "hour_raw",
            "Market Demand": "market_demand_raw",
            "Ontario Demand": "ontario_demand_raw"
        })
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return bronze_df_stream


