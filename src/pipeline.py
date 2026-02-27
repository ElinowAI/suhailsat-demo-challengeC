import argparse
import logging
import os
import sys
import yaml
import pandas as pd
from datetime import datetime

from io_utils import read_dataset, write_dataset
from transform import apply_transformations
from quality import run_quality_checks


def setup_logging(log_level: str):
    os.makedirs("data/logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = f"data/logs/run-{timestamp}.log"

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info("Logging initialized")
    return log_file


def load_config(config_path: str):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main(config_path: str):
    config = load_config(config_path)

    log_file = setup_logging(config["pipeline"]["log_level"])

    logging.info(f"Starting pipeline: {config['pipeline']['name']}")

    df = read_dataset(config["input"])
    logging.info(f"Rows loaded: {len(df)}")

    run_quality_checks(df)

    df = apply_transformations(df, config["processing"])

    logging.info(f"Rows after processing: {len(df)}")

    write_dataset(df, config["output"])

    logging.info("Pipeline completed successfully")
    logging.info(f"Log file saved at: {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    main(args.config)