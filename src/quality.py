import logging


def run_quality_checks(df):
    if df.empty:
        raise ValueError("Input dataset is empty")

    logging.info("Basic quality check passed")