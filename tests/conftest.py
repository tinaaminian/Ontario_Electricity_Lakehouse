import pytest 
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.getOrCreate()
    )
    yield session

    session.stop()