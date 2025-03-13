import csv
import random
import uuid
import argparse
from faker import Faker
import re

fake = Faker()

# Mandatory columns
MANDATORY_COLUMNS = ["Index", "Customer Id",
                     "First Name", "Last Name", "Subscription Date"]

# Possible additional columns (expanded)
OPTIONAL_COLUMNS = [
    "Company", "City", "Country", "Phone", "Email", "Industry", "Revenue", "Zip Code",
    "Address", "State", "Website", "Job Title", "Department",
    "Product Category", "Purchase Date", "Order ID", "Shipping Address",
    "Billing Address", "Credit Card Number", "Social Security Number", 
    "IP Address", "User Agent", "Referral Source", "Language", "Time Zone",
    "Notes", "Comments", "Loyalty Points", "Membership Level", "Last Login",
    "Marketing Opt-In", "Newsletter Subscription", "Preferred Contact Method",
    "Account Status", "Customer Since", "Date of Birth", "Gender", "Ethnicity",
    "Marital Status", "Education Level", "Occupation", "Hobbies", "Interests",
    "Previous Purchases", "Average Order Value", "Customer Rating",
    "Support Tickets", "Last Contacted", "Next Scheduled Contact",
    "Lead Source", "Lead Status", "Campaign ID", "Campaign Name",
    "Ad Group", "Keyword", "Click ID", "Page Views", "Session Duration",
    "Device Type", "Operating System", "Browser", "Screen Resolution",
    "Location Coordinates", "Latitude", "Longitude", "Altitude",
    "Sensor Data", "Temperature", "Humidity", "Pressure", "Acceleration",
]

def normalize_text(text):
    """
    Normalize text by replacing multiple spaces with a single space and removing leading/trailing spaces.
    """
    return re.sub(r'\s+', ' ', text).strip()

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
                    row[col] = normalize_text(fake.company())
                elif col == "City":
                    row[col] = normalize_text(fake.city())
                elif col == "Country":
                    row[col] = normalize_text(fake.country())
                elif col == "Phone":
                    row[col] = normalize_text(fake.phone_number())
                elif col == "Email":
                    row[col] = normalize_text(fake.email())
                elif col == "Industry":
                    row[col] = normalize_text(fake.job())
                elif col == "Revenue":
                    row[col] = random.randint(100000, 10000000)
                elif col == "Zip Code":
                    row[col] = normalize_text(fake.zipcode())
                elif col == "Address":
                    row[col] = normalize_text(fake.address())
                elif col == "State":
                    row[col] = normalize_text(fake.state())
                elif col == "Website":
                    row[col] = normalize_text(fake.url())
                elif col == "Job Title":
                    row[col] = normalize_text(fake.job())
                elif col == "Department":
                    row[col] = fake.random_element(elements=("Sales", "Marketing", "Engineering", "Support"))
                elif col == "Product Category":
                    row[col] = fake.random_element(elements=("Electronics", "Clothing", "Books", "Home Goods"))
                elif col == "Purchase Date":
                    row[col] = fake.date_between(start_date="-1y", end_date="today")
                elif col == "Order ID":
                    row[col] = fake.uuid4()
                elif col == "Shipping Address":
                    row[col] = normalize_text(fake.address())
                elif col == "Billing Address":
                    row[col] = normalize_text(fake.address())
                elif col == "Credit Card Number":
                    row[col] = fake.credit_card_number()  # Be extremely careful with sensitive data
                elif col == "Social Security Number":
                    row[col] = fake.ssn()  # Be extremely careful with sensitive data
                elif col == "IP Address":
                    row[col] = fake.ipv4()
                elif col == "User Agent":
                    row[col] = normalize_text(fake.user_agent())
                elif col == "Referral Source":
                    row[col] = fake.random_element(elements=("Google", "Facebook", "Direct", "Referral"))
                elif col == "Language":
                    row[col] = fake.language_code()
                elif col == "Time Zone":
                    row[col] = normalize_text(fake.timezone())
                elif col == "Notes":
                    row[col] = normalize_text(fake.text())
                elif col == "Comments":
                    row[col] = normalize_text(fake.text())
                elif col == "Loyalty Points":
                    row[col] = random.randint(0, 1000)
                elif col == "Membership Level":
                    row[col] = fake.random_element(elements=("Gold", "Silver", "Bronze"))
                elif col == "Last Login":
                    row[col] = fake.date_time_between(start_date="-1y", end_date="now")
                elif col == "Marketing Opt-In":
                    row[col] = fake.boolean()
                elif col == "Newsletter Subscription":
                    row[col] = fake.boolean()
                elif col == "Preferred Contact Method":
                    row[col] = fake.random_element(elements=("Email", "Phone", "Mail"))
                elif col == "Account Status":
                    row[col] = fake.random_element(elements=("Active", "Inactive", "Pending"))
                elif col == "Customer Since":
                    row[col] = fake.date_between(start_date="-5y", end_date="today")
                elif col == "Date of Birth":
                    row[col] = fake.date_between(start_date="-65y", end_date="-18y")
                elif col == "Gender":
                    row[col] = fake.random_element(elements=("Male", "Female", "Other"))
                elif col == "Ethnicity":
                    row[col] = fake.random_element(elements=("Caucasian", "Hispanic", "Asian", "African American"))
                elif col == "Marital Status":
                    row[col] = fake.random_element(elements=("Single", "Married", "Divorced"))
                elif col == "Education Level":
                    row[col] = fake.random_element(elements=("High School", "Bachelor's", "Master's", "PhD"))
                elif col == "Occupation":
                    row[col] = normalize_text(fake.job())
                elif col == "Hobbies":
                    row[col] = fake.random_element(elements=("Reading", "Hiking", "Gaming", "Cooking"))
                elif col == "Interests":
                    row[col] = fake.random_element(elements=("Technology", "Travel", "Music", "Sports"))
                elif col == "Previous Purchases":
                    row[col] = random.randint(0, 100)
                elif col == "Average Order Value":
                    row[col] = round(random.uniform(10, 1000), 2)
                elif col == "Customer Rating":
                    row[col] = random.randint(1, 5)
                elif col == "Support Tickets":
                    row[col] = random.randint(0, 10)
                elif col == "Last Contacted":
                    row[col] = fake.date_time_between(start_date="-6m", end_date="now")
                elif col == "Next Scheduled Contact":
                    row[col] = fake.date_time_between(start_date="now", end_date="+6m")
                elif col == "Lead Source":
                    row[col] = fake.random_element(elements=("Website", "Referral", "Advertisement"))
                elif col == "Lead Status":
                    row[col] = fake.random_element(elements=("New", "Contacted", "Qualified", "Converted"))
                elif col == "Campaign ID":
                    row[col] = fake.uuid4()
                elif col == "Campaign Name":
                    row[col] = normalize_text(fake.catch_phrase())
                elif col == "Ad Group":
                    row[col] = normalize_text(fake.word())
                elif col == "Keyword":
                    row[col] = normalize_text(fake.word())
                elif col == "Click ID":
                    row[col] = fake.uuid4()
                elif col == "Page Views":
                    row[col] = random.randint(0, 100)
                elif col == "Session Duration":
                    row[col] = random.randint(60, 3600)
                elif col == "Device Type":
                    row[col] = fake.random_element(elements=("Desktop", "Mobile", "Tablet"))
                elif col == "Operating System":
                    row[col] = fake.random_element(elements=("Windows", "macOS", "Linux", "Android", "iOS"))
                elif col == "Browser":
                    row[col] = fake.random_element(elements=("Chrome", "Firefox", "Safari", "Edge"))
                elif col == "Screen Resolution":
                    row[col] = fake.random_element(elements=("1920x1080", "1366x768", "1280x720"))
                elif col == "Location Coordinates":
                    row[col] = f"{fake.latitude()}, {fake.longitude()}"
                elif col == "Latitude":
                    row[col] = fake.latitude()
                elif col == "Longitude":
                    row[col] = fake.longitude()
                elif col == "Altitude":
                    row[col] = random.randint(0, 8000)
                elif col == "Sensor Data":
                    row[col] = f"{random.uniform(0, 100)}, {random.uniform(0, 100)}"
                elif col == "Temperature":
                    row[col] = random.uniform(-20, 40)
                elif col == "Humidity":
                    row[col] = random.uniform(0, 100)
                elif col == "Pressure":
                    row[col] = random.uniform(900, 1100)
                elif col == "Acceleration":
                    row[col] = f"{random.uniform(-1, 1)}, {random.uniform(-1, 1)}, {random.uniform(-1, 1)}"
            
            writer.writerow(row)

    print(f"Generated: {filename}")


if __name__ == "__main__":
    # Generate multiple CSVs with different structures
    parser = argparse.ArgumentParser(description='Generate random customer data CSV files')
    parser.add_argument('--start', type=int, default=1, help='Start index for file generation')
    parser.add_argument('--end', type=int, default=20, help='End index for file generation')
    parser.add_argument('--records', type=int, default=10, help='Maximum number of records per file')
    
    args = parser.parse_args()
    
    for i in range(args.start, args.end + 1):  # Creating customers-[args.start].csv to customers-[args.end].csv
        generate_random_csv(i, num_records=random.randint(1, args.records)) # 1 to [args.records] records per file
