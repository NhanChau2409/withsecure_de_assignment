from typing import List

import pandas as pd

from constant import *


def batching_records(
    records: List[str],
    max_record_size: int,
    max_batch_size: int,
    max_num_records: int,
) -> List[List[str]]:
    """
    Groups records into batches based on size and count constraints.

    Args:
        records (List[str]): List of records to be batched.
        max_record_size (int): Maximum allowed size for each record in bytes.
        max_batch_size (int): Maximum  size of each a batch in bytes.
        max_num_records (int): Maximum number of records in a batch.

    Returns:
        List[List[str]]: A list of batched records where each batch is a list of strings.
    """
    df = pd.DataFrame({RECORD_COL_NAME: records}).dropna()

    # Add record size column
    df[RECORD_SIZE_COL_NAME] = df[RECORD_COL_NAME].apply(
        lambda record: len(record.encode("utf-8"))
    )

    # Filter out record > max_record_size
    df = df.where(df[RECORD_SIZE_COL_NAME] < max_record_size).dropna()

    # Assign group key for records by batch size < max_batch_size
    df[SIZE_GK] = df[RECORD_SIZE_COL_NAME].cumsum().floordiv(max_batch_size)

    # Assign group key for records by number of records in a batch < max_num_records
    df[COUNT_GK] = df.index.to_series().floordiv(max_num_records)

    return df.groupby([SIZE_GK, COUNT_GK])[RECORD_COL_NAME].agg(list).to_list()
