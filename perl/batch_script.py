#!/usr/bin/env python3
import os
from datetime import datetime, timedelta
import sys

# ---- CONFIG ----
FILE_TYPES = [
    {"Name": "LE", "Count": 10000},
    {"Name": "CD", "Count": 10000},
    {"Name": "HR", "Count": 2000},
    {"Name": "TL", "Count": 2000}
]

START_DATE = datetime(2025, 10, 18)  # Initial date
# ----------------

def main(input_txt_path):
    if not os.path.exists(input_txt_path):
        print(f"Input TXT file not found: {input_txt_path}")
        sys.exit(1)

    input_dir = os.path.dirname(input_txt_path)
    output_root = os.path.join(input_dir, "October")
    os.makedirs(output_root, exist_ok=True)
    print(f"Output root directory: {output_root}")

    # Read all phone numbers
    with open(input_txt_path, "r", encoding="utf-8") as f:
        phone_numbers = [line.strip() for line in f if line.strip()]
    total_numbers = len(phone_numbers)
    print(f"Total phone numbers found: {total_numbers}")

    numbers_per_batch = sum(ft["Count"] for ft in FILE_TYPES)
    print(f"Numbers needed per batch: {numbers_per_batch}")

    current_index = 0
    batch_number = 1
    current_date = START_DATE

    while current_index < total_numbers:
        remaining_numbers = total_numbers - current_index
        if remaining_numbers < numbers_per_batch:
            print(f"Insufficient numbers for complete batch. Remaining: {remaining_numbers}")
            break

        # Skip Fridays
        while current_date.weekday() == 4:  # Friday = 4
            current_date += timedelta(days=1)

        # Folder for this date (e.g. "September/11")
        day_folder = os.path.join(output_root, str(current_date.day))
        os.makedirs(day_folder, exist_ok=True)

        date_str = current_date.strftime("%d%B%y")  # e.g. 11September25
        print(f"\nCreating batch {batch_number} in folder: {day_folder}")

        for ft in FILE_TYPES:
            file_name = f"{date_str}{ft['Name']}.csv"
            file_path = os.path.join(day_folder, file_name)

            end_index = current_index + ft["Count"]
            numbers_for_file = phone_numbers[current_index:end_index]

            with open(file_path, "w", encoding="utf-8") as out:
                out.write("\n".join(numbers_for_file))

            print(f"  Created: {file_name} with {ft['Count']} numbers")
            current_index = end_index

        # Move to next day
        current_date += timedelta(days=1)
        while current_date.weekday() == 4:  # Skip Friday
            current_date += timedelta(days=1)

        batch_number += 1

    remaining = total_numbers - current_index
    print("\nProcess completed!")
    print(f"Total batches created: {batch_number - 1}")
    print(f"Total numbers processed: {current_index}")
    print(f"Remaining numbers: {remaining}")
    if remaining > 0:
        print("Note: Remaining numbers were not processed due to insufficient quantity for a complete batch.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 split_batches.py <input_file.txt>")
        sys.exit(1)
    main(sys.argv[1])
