#!/usr/bin/env python3
import os
from datetime import datetime, timedelta

# ---- CONFIG ----
FILE_TYPES = [
    {"Name": "LE", "Count": 10000},
    {"Name": "CD", "Count": 10000},
    {"Name": "HR", "Count": 2000},
    {"Name": "TL", "Count": 2000}
]

START_DATE = datetime(2025, 10, 28)  # Initial date
# ----------------

def main():
    # Load scrub.txt from current folder
    scrub_file_path = os.path.join(os.getcwd(), "scrub.txt")
    if not os.path.exists(scrub_file_path):
        print("❌ scrub.txt not found in current folder:", os.getcwd())
        return

    # Read all phone numbers
    with open(scrub_file_path, "r", encoding="utf-8") as f:
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
        while current_date.weekday() == 4:
            current_date += timedelta(days=1)

        # Folder name: Year + Month abbreviation e.g., 2025Oct
        month_folder = current_date.strftime("%Y%b")
        day_folder = os.path.join(os.getcwd(), month_folder, str(current_date.day))
        os.makedirs(day_folder, exist_ok=True)

        date_str = current_date.strftime("%d%B%y")
        print(f"\nCreating batch {batch_number} in folder: {day_folder}")

        batch_numbers = []  # Track numbers used in this batch

        for ft in FILE_TYPES:
            file_name = f"{date_str}{ft['Name']}.csv"
            file_path = os.path.join(day_folder, file_name)

            end_index = current_index + ft["Count"]
            numbers_for_file = phone_numbers[current_index:end_index]

            with open(file_path, "w", encoding="utf-8") as out:
                out.write("\n".join(numbers_for_file))

            print(f"  Created: {file_name} with {ft['Count']} numbers")
            batch_numbers.extend(numbers_for_file)
            current_index = end_index

        # Remove used numbers from phone_numbers
        phone_numbers = phone_numbers[current_index:]
        current_index = 0  # reset index for remaining numbers
        total_numbers = len(phone_numbers)

        # Update scrub.txt after each batch
        with open(scrub_file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(phone_numbers))
            if phone_numbers:
                f.write("\n")

        # Move to next day
        current_date += timedelta(days=1)
        while current_date.weekday() == 4:
            current_date += timedelta(days=1)

        batch_number += 1

    remaining = len(phone_numbers)
    print("\nProcess completed!")
    print(f"Total batches created: {batch_number - 1}")
    print(f"Total numbers processed: {sum(ft['Count'] for ft in FILE_TYPES)*(batch_number-1)}")
    print(f"Remaining numbers in scrub.txt: {remaining}")
    if remaining > 0:
        print("Note: Remaining numbers were not processed due to insufficient quantity for a complete batch.")

if __name__ == "__main__":
    main()
