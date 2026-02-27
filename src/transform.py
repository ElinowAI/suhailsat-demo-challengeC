import pandas as pd
import logging


def apply_transformations(df, config):
    if "rename_columns" in config:
        df = df.rename(columns=config["rename_columns"])
        logging.info("Columns renamed")

    if "cast_types" in config:
        for col, dtype in config["cast_types"].items():
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col])
            else:
                df[col] = df[col].astype(dtype)
        logging.info("Types casted")

    if "drop_nulls" in config:
        df = df.dropna(subset=config["drop_nulls"]["subset"])
        logging.info("Null values dropped")

    if "drop_duplicates" in config:
        df = df.drop_duplicates(subset=config["drop_duplicates"]["subset"])
        logging.info("Duplicates dropped")

    if "filters" in config:
        for rule in config["filters"]:
            col = rule["column"]
            val = rule["value"]
            op = rule["operator"]

            if op == ">=":
                df = df[df[col] >= val]
            elif op == "<=":
                df = df[df[col] <= val]
        logging.info("Filters applied")

    return df