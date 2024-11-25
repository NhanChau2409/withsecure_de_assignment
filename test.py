import logging
import unittest
from typing import List

import pandas as pd

from batching_records import batching_records
from generate_records import generate_records_with_percentage


class TestBatchingRecords(unittest.TestCase):
    def setUp(self):
        self.total_records = 1_000
        self.min_size = 100
        self.record_size = 1023  # in bytes
        self.oversized_percentage = 10  # percent
        self.max_record_size = 1024**2  # 1 MB
        self.max_batch_size = 5 * 1024**2  # 5 MB
        self.max_num_records = 500

        self.records = generate_records_with_percentage(
            total_records=self.total_records,
            min_size=self.min_size,
            max_size=self.record_size,
            oversized_percentage=self.oversized_percentage,
        )

        self.batches_series = pd.Series(
            batching_records(
                self.records,
                max_record_size=self.max_record_size,
                max_batch_size=self.max_batch_size,
                max_num_records=self.max_num_records,
            )
        )

    def test_no_record_exceeds_max_size(self):
        """Ensure no individual record exceeds the maximum allowed size."""
        self.assertTrue(
            (
                self.batches_series.explode().apply(
                    lambda record: len(record.encode("utf-8"))
                )
                < self.max_record_size
            ).all(),
            f"Record size exceeds maximum allowed {self.max_record_size} bytes",
        )

    def test_no_batch_exceeds_max_size(self):
        """Ensure no batch exceeds the maximum allowed batch size."""
        self.assertTrue(
            (
                self.batches_series.apply(
                    lambda batch: sum([len(record.encode("utf-8")) for record in batch])
                )
                < self.max_batch_size
            ).all(),
            f"Batch size exceeds maximum allowed {self.max_batch_size} bytes",
        )

    def test_no_batch_exceeds_max_num_records(self):
        """Ensure no batch exceeds the maximum number of allowed records."""
        self.assertTrue(
            (self.batches_series.count() < self.max_num_records).all(),
            f"Batch exceeds maximum allowed {self.max_num_records} records",
        )


if __name__ == "__main__":
    unittest.main()
