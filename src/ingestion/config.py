from dataclasses import dataclass

@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    file_path: str
    target_table: str
    file_format = "csv" 

IESO_DEMAND_2025 = SourceConfig(
        source_name= "ieso",
        file_path= "/Volumes/ontario_energy/landing/source_file/PUB_Demand_2025.csv",
        target_table= "ontario_energy.bronze.ieso_demand"
    )