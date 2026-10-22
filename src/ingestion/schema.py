from pyspark.sql.types import (StructType, StructField,StringType)

IESO_DEMAND_BRONZE_SCHEMA = StructType([
StructField("Date", StringType(), True ),
StructField("Hour", StringType(), True),
StructField("Market Demand", StringType(), True),
StructField("Ontario Demand", StringType(), True),
])