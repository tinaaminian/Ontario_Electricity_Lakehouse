from pathlib import Path
from src.ingestion.bronze_loader import read_source
from tests.conftest import spark
from src.ingestion.config import SourceConfig
import pytest

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

def test_read_source_valid_csv(spark):
    config = SourceConfig(
        source_name= "ieso_test",
        file_path= str(FIXTURE_DIR/ "ieso_valid.csv"),
        target_table= "test_bronze"
    )
    df= read_source(spark,config)
    assert df.count() == 3
    assert df.columns == [
        "Date",
        "Hour",
        "Market Demand",
        "Ontario Demand"
    ]

def test_read_source_rejectes_reordered_headers(spark):
    config = SourceConfig(
        source_name= "ieso_test",
        file_path= str(FIXTURE_DIR / "ieso_reordered.csv"),
        target_table= "test_bronze"

    )
    with pytest.raises(Exception):
        df = read_source(spark,config)
        df.collect()

def test_read_source_header_only_csv(spark):
        config= SourceConfig(
            source_name= "ieso_test",
            file_path= str(FIXTURE_DIR / "ieso_header_only.csv"),
            target_table= "test_bronze"
        )
        df = read_source(spark,config)
        assert df.count() == 0
        assert df.columns == [
            "Date",
            "Hour",
            "Market Demand",
            "Ontario Demand"
        ]

def test_read_source_bad_rows(spark):
    config = SourceConfig(
        source_name = "ieso_test",
        file_path = str(FIXTURE_DIR / "ieso_bad_row.csv"),
        target_table= "test_bronze"
    )
    assert (FIXTURE_DIR / "ieso_bad_row.csv").is_file()
    with pytest.raises(Exception):
        df = read_source(spark,config)
        df.collect()