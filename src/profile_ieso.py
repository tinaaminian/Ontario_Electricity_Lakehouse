from pathlib import Path 

#CSV File Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] 
CSV_FILE = PROJECT_ROOT/ "data" /"raw" / "PUB_Demand_2025.csv"

def main():
    if not CSV_FILE.exists():
        raise FileNotFoundError("the requested file is not found")
# Inspect the beginning of a source file
    with CSV_FILE.open(mode="r", encoding="utf-8") as f:
        for line_number, line in enumerate(f,start=1):
            if line_number > 15:
                break
            print(f"{line_number}:{line.rstrip()}")

if __name__=="__main__":
    main()

