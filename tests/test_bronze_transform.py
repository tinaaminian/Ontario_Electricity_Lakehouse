
from datetime import datetime
from uuid import uuid4

from src.ingestion.bronze_loader import prepare_bronze
from src.ingestion.config import SourceConfig


def test_prepare_bronze_renames_columns(spark):
    """Verify source column renaming and the expected Bronze schema."""

    # ARRANGE: Create a representative raw IESO demand record.
    data = [
        ("2025-01-01", "1", "16500", "14800")
    ]

    columns = [
        "Date",
        "Hour",
        "Market Demand",
        "Ontario Demand",
    ]

    df = spark.createDataFrame(data, columns)

    # Configure a sample source path and ingestion batch identifier.
    config = SourceConfig(
        source_name="ieso_test",
        file_path="/tmp/PUB_Demand_2025.csv",
        target_table="test_bronze",
    )

    batch_id = str(uuid4())

    # ACT: Apply the Bronze transformation.
    result = prepare_bronze(df, config, batch_id)

    # ASSERT: Verify the exact output schema, including metadata columns.
    assert result.columns == [
        "date_raw",
        "hour_raw",
        "market_demand_raw",
        "ontario_demand_raw",
        "source_file_path",
        "source_file_name",
        "batch_id",
        "ingested_at",
    ]


def test_prepare_bronze_preserves_values(spark):
    """Verify that Bronze transformation preserves all source records and values."""

    # ARRANGE: Create two source records with known demand values.
    data = [
        ("2025-01-01", "1", "16500", "14800"),
        ("2025-01-01", "2", "16000", "14300"),
    ]

    columns = [
        "Date",
        "Hour",
        "Market Demand",
        "Ontario Demand",
    ]

    df = spark.createDataFrame(data, columns)

    config = SourceConfig(
        source_name="ieso_test",
        file_path="/tmp/PUB_Demand_2025.csv",
        target_table="test_bronze",
    )

    batch_id = "test-batch-001"

    # ACT: Transform the source records into Bronze format.
    result = prepare_bronze(df, config, batch_id)

    # ASSERT: Extract only the original data fields, excluding metadata.
    actual = [
        (
            row["date_raw"],
            row["hour_raw"],
            row["market_demand_raw"],
            row["ontario_demand_raw"],
        )
        for row in result.collect()
    ]

    # Compare records independently of Spark's nondeterministic row order.
    assert sorted(actual) == sorted(data)


def test_prepare_bronze_adds_metadata(spark):
    """Verify that Bronze records contain correct ingestion lineage metadata."""

    # ARRANGE: Create a source record and define its expected lineage.
    data = [
        ("2025-01-01", "1", "16500", "14800")
    ]

    columns = [
        "Date",
        "Hour",
        "Market Demand",
        "Ontario Demand",
    ]

    df = spark.createDataFrame(data, columns)

    config = SourceConfig(
        source_name="ieso_test",
        file_path="/tmp/PUB_Demand_2025.csv",
        target_table="test_bronze",
    )

    batch_id = "test-batch-001"

    # ACT: Add ingestion metadata through the Bronze transformation.
    result = prepare_bronze(df, config, batch_id)

    # ASSERT: Retrieve the transformed record for metadata validation.
    row = result.collect()[0]

    # Verify that the source location and filename are recorded correctly.
    assert row["source_file_path"] == config.file_path
    assert row["source_file_name"] == "PUB_Demand_2025.csv"

    # Verify that the provided batch identifier is preserved.
    assert row["batch_id"] == batch_id

    # Verify that Spark generates a valid ingestion timestamp.
    assert isinstance(row["ingested_at"], datetime)
