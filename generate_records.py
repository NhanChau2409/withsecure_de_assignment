import random
import string


def generate_records_with_percentage(
    total_records=5000, min_size=100, max_size=1023, oversized_percentage=0
):
    """
    Generates a list of string records, with a specified percentage exceeding a certain size.

    Parameters:
    - total_records (int): Total number of records to generate.
    - min_size (int): Minimum size of each record in bytes.
    - max_size (int): Maximum size of each record in bytes (default is 1023 for <1 KB).
    - oversized_percentage (int): Percentage of records that should exceed max_size.

    Returns:
    - List[str]: A list containing the generated string records.
    """
    records = []
    oversized_count = (total_records * oversized_percentage) // 100

    for i in range(total_records):
        if i < oversized_count:
            # Generate oversized record
            size = random.randint(
                max_size + 1, max_size + 1024
            )  # Ensure it's oversized
        else:
            # Random size between min_size and max_size bytes
            size = random.randint(min_size, max_size)

        # Generate a random string of the specified size
        record = "".join(random.choices(string.ascii_letters + string.digits, k=size))
        records.append(record)

    return records


if __name__ == "__main__":
    # Parameters
    total_records = 5000  # Total number of records to generate
    min_size = 1  # Minimum size of each record in bytes
    max_size = 800  # Maximum size of each record in bytes (just under 1 KB)
    oversized_percentage = 10  # Percentage of records that should be oversized

    # Generate the records
    records = generate_records_with_percentage(
        total_records=total_records,
        min_size=min_size,
        max_size=max_size,
        oversized_percentage=oversized_percentage,
    )

    # Optional: Verify the distribution
    oversized_records = [rec for rec in records if len(rec) >= 1024]
    undersized_records = [rec for rec in records if len(rec) < 1024]

    print(f"Total records generated: {len(records)}")
    print(f"Number of records < 1 KB: {len(undersized_records)}")
    print(
        f"Number of oversized records (should be {oversized_percentage}%): {len(oversized_records)}"
    )

    # Optional: Display sample records
    # for i, rec in enumerate(records[:5], 1):
    #     print(f"Record {i} ({len(rec)} bytes): {rec}")
