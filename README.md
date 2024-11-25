# Batching Records Library

## Purpose

The library would be useful in an application which continuously reads large numbers of records from a data source and writes them to an AWS Kinesis Data stream. The library could be used to create optimum batches for sending data to the Kinesis stream

## Requirements

To run this project, you will need the following:

- Python 3.10 or higher
- Required Python packages:
  - `pandas`: For data manipulation and analysis.

You can install the required packages using pip:

```bash
pip install pandas
```

## Overview of the Batching Logic

The core functionality of this project is encapsulated in the `batching_records` function, which takes a list of records and groups them into batches based on three main constraints:

1. **Maximum Record Size**: Each individual record in a batch must not exceed a specified size in bytes.
2. **Maximum Batch Size**: The total size of all records in a batch must not exceed a specified size in bytes.
3. **Maximum Number of Records**: The number of records in a batch must not exceed a specified count.

### Logic of `batching_records` Function

The `batching_records` function operates as follows:

1. **Input Parameters**:

   - `records`: A list of string records to be batched.
   - `max_record_size`: The maximum allowed size for each record in bytes.
   - `max_batch_size`: The maximum size of each batch in bytes.
   - `max_num_records`: The maximum number of records allowed in a single batch.

2. **DataFrame Creation**:

   - The function begins by creating a Pandas DataFrame from the list of records. This DataFrame will facilitate the manipulation and analysis of the records.

3. **Record Size Calculation**:

   - A new column is added to the DataFrame that calculates the size of each record in bytes using UTF-8 encoding.

4. **Filtering Records**:

   - Records that exceed the `max_record_size` are filtered out, ensuring that only valid records are considered for batching.

5. **Group Key Assignment**:

   - Two group keys are assigned to the records:
     - `SIZE_GK`: This key groups records based on their cumulative size, ensuring that the total size of records in a batch does not exceed `max_batch_size`.
     - `COUNT_GK`: This key groups records based on their index, ensuring that the number of records in a batch does not exceed `max_num_records`.

6. **Batching**:
   - Finally, the function groups the records by the two keys (`SIZE_GK` and `COUNT_GK`) and aggregates them into lists, resulting in a list of batches where each batch is a list of records.

### Example Usage

To use the `batching_records` function, you can call it with the required parameters as shown below:

```python
batches = batching_records(
    records=["record1", "record2", ...],
    max_record_size=1024**2,
    max_batch_size=5 * 1024**2,
    max_num_records=500
)
```

This will return a list of batches, each containing records that adhere to the specified constraints.

## Conclusion

The Batching Records project provides a robust solution for managing large datasets by efficiently grouping records into batches. This can significantly improve performance and resource management in data processing tasks.
