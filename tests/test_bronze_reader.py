from src.profile_ieso import CSV_FILE
from src.ingestion.bronze_loader import (read_source,prepare_bronze)
from pyspark.sql import SparkSession
from src.ingestion.config import IESO_DEMAND_2025
from dataclasses import replace
from uuid import uuid4


def main() -> None:

    #create a local configuration
    local_conf = replace(IESO_DEMAND_2025, file_path = str(CSV_FILE))

    #create a local spark session
    spark = SparkSession.builder.getOrCreate()

    try:
        #read the csv
         df = read_source(spark,local_conf)

         #Inspect the data
         df.printSchema()
         df.show()
         print(f"Total rows are:{df.count()}")
         batch_id = str(uuid4())

         print("displaying bronze dataframe")
         df_bronze = prepare_bronze(df,local_conf,batch_id)
         df_bronze.show()
         print(f"number of rowa are {df_bronze.count()}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
   