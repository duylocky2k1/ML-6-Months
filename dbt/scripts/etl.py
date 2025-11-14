#!/usr/bin/env python3
import os
import logging
import subprocess
from pathlib import Path
import pandas as pd
import requests

# -----------------------------
# Config
# -----------------------------
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CSV_PATH = DATA_DIR / "iris.csv"
URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# Step 1: Download dataset
# -----------------------------
def download_dataset(url: str, save_path: Path):
    logging.info(f"Downloading dataset from {url}")
    r = requests.get(url)
    if r.status_code == 200:
        data = r.text.splitlines()
        # Add header
        df = pd.DataFrame([x.split(',') for x in data if x], columns=[
            "SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm", "Species"
        ])
        df.index += 1
        df.to_csv(save_path, index=False)
        logging.info(f"Saved CSV to {save_path}")
    else:
        logging.error(f"Failed to download dataset. Status code: {r.status_code}")
        raise Exception("Download failed")
                     

# -----------------------------
# Step 2: Run dbt
# -----------------------------
def run_dbt(commands: list):
    for cmd in commands:
        logging.info(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            logging.error(f"Command failed: {cmd}")
            raise Exception("dbt command failed")

# -----------------------------
# Main CLI
# -----------------------------
if __name__ == "__main__":
    # Step 1: download and create CSV
    download_dataset(URL, CSV_PATH)

    # Step 2: run dbt pipeline + test
    run_dbt([
        "dbt seed --full-refresh",
        "dbt run --full-refresh",
        "dbt test"
    ])
    logging.info("ETL + dbt pipeline completed!")


