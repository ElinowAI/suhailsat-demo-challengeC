import argparse
import logging
import os
import sys
import time
import yaml
import pandas as pd
from datetime import datetime

from io_utils import read_dataset, write_dataset
from transform import apply_transformations
from quality import run_quality_checks


def setup_logging(log_level: str):
    #I create a dedicated logs folder to keep execution artifacts separated from source code
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
    #Config is fully externalized to ensure configuration-driven execution
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main(config_path: str):

    #I measure total runtime to provide basic observability metrics
    start_time = time.perf_counter()

    config = load_config(config_path)

    log_file = setup_logging(config["pipeline"]["log_level"])

    #I log runtime metadata to guarantee reproducibility across environments
    logging.info(f"Python version: {sys.version}")
    logging.info(f"Pandas version: {pd.__version__}")

    pipeline_name = config["pipeline"]["name"]
    input_path = config["input"]["path"]
    input_format = config["input"]["format"]
    output_path = config["output"]["path"]
    output_format = config["output"]["format"]

    logging.info(f"Starting pipeline: {pipeline_name}")
    logging.info(f"Input source: {input_path} ({input_format})")

    # ----------------------
    # EXTRACT
    # ----------------------
    df = read_dataset(config["input"])
    rows_in = len(df)
    logging.info(f"Rows loaded: {rows_in}")

    # ----------------------
    # DATA QUALITY
    # ----------------------
    run_quality_checks(df)

    # ----------------------
    # TRANSFORM
    # ----------------------
    df = apply_transformations(df, config["processing"])
    rows_out = len(df)

    logging.info(f"Rows after processing: {rows_out}")
    logging.info(f"Rows removed during processing: {rows_in - rows_out}")

    # ----------------------
    # LOAD
    # ----------------------
    write_dataset(df, config["output"])

    #I log output destination explicitly to improve traceability
    logging.info(f"Output destination: {output_path} ({output_format})")

    # ----------------------
    # SUMMARY
    # ----------------------
    duration = round(time.perf_counter() - start_time, 4)

    logging.info("Pipeline completed successfully")
    logging.info("---- Run Summary ----")
    logging.info(f"Pipeline: {pipeline_name}")
    logging.info(f"Input rows: {rows_in}")
    logging.info(f"Output rows: {rows_out}")
    logging.info(f"Execution time: {duration} seconds")
    logging.info(f"Log file: {log_file}")
    logging.info("---------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    try:
        main(args.config)
    except Exception:
        #I added this block to ensure proper failure signaling and CI/CD compatibility
        logging.exception("Pipeline failed")
        sys.exit(1)