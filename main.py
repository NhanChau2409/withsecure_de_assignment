import logging
import time

from batching_records import *
from constant import *
from generate_records import *

if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    total_records = 100_000
    min_size = 100
    max_size = 1023
    oversized_percentage = 10

    start_time = time.time()
    batching_records(
        generate_records_with_percentage(
            total_records,
            min_size,
            max_size,
            oversized_percentage,
        ),
        DEFAULT_MAX_RECORD_SIZE,
        DEFAULT_MAX_BATCH_SIZE,
        DEFAULT_MAX_NUM_RECORDS,
    )
    end_time = time.time()

    logging.info(
        (
            f"Successfully batching {total_records} records with "
            f"min size {min_size} bytes, "
            f"max size {max_size} bytes and oversized_percentage {oversized_percentage}% "
            f"took {end_time - start_time:.2f} seconds."
        )
    )
