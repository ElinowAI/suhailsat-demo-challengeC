import pandas as pd
import logging


def read_dataset(config):
    if config["format"] == "csv":
        return pd.read_csv(config["path"])
    elif config["format"] == "json":
        return pd.read_json(config["path"])
    else:
        raise ValueError("Unsupported format")


def write_dataset(df, config):
    if config["format"] == "csv":
        df.to_csv(config["path"], index=False)
    else:
        raise ValueError("Unsupported output format")

    logging.info(f"Output written to {config['path']}")