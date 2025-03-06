import csv
import random
import uuid
from faker import Faker

fake = Faker()

# Mandatory columns
MANDATORY_COLUMNS = ["Index", "Customer Id",
                     "First Name", "Last Name", "Subscription Date"]

# Possible additional columns
OPTIONAL_COLUMNS = [
    "Company", "City", "Country", "Phone", "Email", "Industry", "Revenue", "Zip Code"
]


def generate_random_csv(file_index, num_records=1):
    """
    Generate a CSV file with random customer data, adding new fields dynamically.

    :param file_index: The index for the CSV filename.
    :param num_records: Number of records to generate in the file.
    """
    # Randomly choose how many extra fields to include
    selected_optional_columns = random.sample(
        OPTIONAL_COLUMNS, k=random.randint(1, len(OPTIONAL_COLUMNS)))

    # Final column list
    all_columns = MANDATORY_COLUMNS + selected_optional_columns

    # File name
    filename = f"data/raw/customers-{file_index}.csv"

    # Generate data
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=all_columns)
        writer.writeheader()

        for i in range(1, num_records + 1):
            row = {
                "Index": file_index * 10 + i,  # Ensuring unique Index values
                "Customer Id": fake.uuid4()[:16],  # Shortened UUID
                "First Name": fake.first_name(),
                "Last Name": fake.last_name(),
                "Subscription Date": fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d"),
            }

            # Fill optional fields dynamically
            for col in selected_optional_columns:
                if col == "Company":
                    row[col] = fake.company()
                elif col == "City":
                    row[col] = fake.city()
                elif col == "Country":
                    row[col] = fake.country()
                elif col == "Phone":
                    row[col] = fake.phone_number()
                elif col == "Email":
                    row[col] = fake.email()
                elif col == "Industry":
                    row[col] = fake.job()
                elif col == "Revenue":
                    row[col] = random.randint(100000, 10000000)
                elif col == "Zip Code":
                    row[col] = fake.zipcode()

            writer.writerow(row)

    print(f"Generated: {filename}")


if __name__ == "__main__":
    # Generate multiple CSVs with different structures
    for i in range(21, 22):  # Creating customers-1.csv to customers-20.csv
        generate_random_csv(i, num_records=random.randint(1, 10))
