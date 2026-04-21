import time
import json
import logging
from datetime import datetime

from database import init_db, insert_log
from logger_config import setup_logger

import weather_pipeline
import news_pipeline
import crypto_pipeline

PIPELINE_MAP = {
    "weather": weather_pipeline.run,
    "news": news_pipeline.run,
    "crypto": crypto_pipeline.run
}


def load_config():
    with open("config.json") as f:
        return json.load(f)


def validate_config(config):
    if "pipeline_order" not in config:
        raise ValueError("Missing pipeline_order in config")

    for pipeline in config["pipeline_order"]:
        if pipeline not in PIPELINE_MAP:
            raise ValueError(f"Invalid pipeline defined: {pipeline}")


def execute_pipeline(name, api_url):
    start_time = datetime.now()
    start = time.time()

    status = "Success"
    output_location = None
    error_message = None

    try:
        output_location = PIPELINE_MAP[name].run(api_url)
        logging.info(f"{name} pipeline executed successfully.")
    except Exception as e:
        status = "Failure"
        error_message = str(e)
        logging.error(f"{name} pipeline failed: {error_message}")

    end = time.time()
    end_time = datetime.now()
    runtime = end - start

    insert_log((
        name,
        str(start_time),
        str(end_time),
        runtime,
        status,
        output_location,
        error_message
    ))

    return runtime, status


def main():
    setup_logger()
    init_db()

    config = load_config()
    validate_config(config)

    total_start = time.time()
    summary = []

    for pipeline in config["pipeline_order"]:
        api_key = f"{pipeline}_api"
        runtime, status = execute_pipeline(pipeline, config[api_key])
        summary.append((pipeline, status, runtime))

    total_runtime = time.time() - total_start

    print("\n===== EXECUTION SUMMARY =====")
    print(f"Total Runtime: {total_runtime:.2f} seconds\n")

    for pipeline, status, runtime in summary:
        print(f"{pipeline.upper()} -> {status} | Runtime: {runtime:.2f} sec")

    print("\nLogs stored in SQLite DB and logs/pipeline.log")


if __name__ == "__main__":
    main()