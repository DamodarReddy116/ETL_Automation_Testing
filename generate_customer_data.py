from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_FILE = Path(__file__).resolve().parent / "data" / "customer_source.csv"
random.seed(42)

VALID_STATUSES = ["ACTIVE", "INACTIVE", "PENDING", "NEW"]
INVALID_STATUSES = ["BLOCKED", "SUSPENDED", "", "UNKNOWN"]
FIRST_NAMES = [
    "John", "Mary", "Alex", "Priya", "Daniel", "Olivia", "Noah", "Emma", "Liam", "Sophia",
    "Michael", "Isabella", "Ethan", "Charlotte", "Mason", "Ava", "Lucas", "Mia", "Benjamin",
    "Harper", "Jacob", "Ella", "James", "Amelia", "Henry", "Grace", "Leo", "Chloe", "Jack",
]
LAST_NAMES = [
    "Smith", "Jones", "Brown", "Johnson", "Williams", "Davis", "Miller", "Wilson", "Moore",
    "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia",
    "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen",
]
DOMAINS = ["company.com", "example.org", "delta.io", "northwind.ai", "retailgroup.net"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
SOURCE_SYSTEMS = ["CRM", "ERP", "WEB", "POS"]


def random_date(year_start: int = 2020, year_end: int = 2025) -> str:
    start = datetime(year_start, 1, 1)
    end = datetime(year_end, 12, 31)
    delta_days = (end - start).days
    chosen_day = start + timedelta(days=random.randint(0, delta_days))
    return chosen_day.strftime("%Y-%m-%d")


def rows_for_scenario(record_number: int):
    if record_number % 7 == 0:
        return {
            "customer_id": "",
            "first_name": "",
            "last_name": "Guest",
            "email": "",
            "status": random.choice(INVALID_STATUSES),
            "signup_date": random_date(),
            "amount": round(random.uniform(-1500, 800), 2),
            "updated_at": (datetime.strptime(random_date(), "%Y-%m-%d") + timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d %H:%M:%S"),
            "region": random.choice(REGIONS),
            "source_system": random.choice(SOURCE_SYSTEMS),
        }
    if record_number % 11 == 0:
        return {
            "customer_id": f"C{(record_number % 12000) + 1:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": "invalid-email",
            "status": random.choice(VALID_STATUSES),
            "signup_date": random_date(),
            "amount": round(random.uniform(-500, 3500), 2),
            "updated_at": (datetime.strptime(random_date(), "%Y-%m-%d") + timedelta(days=random.randint(1, 220))).strftime("%Y-%m-%d %H:%M:%S"),
            "region": random.choice(REGIONS),
            "source_system": random.choice(SOURCE_SYSTEMS),
        }
    if record_number % 13 == 0:
        return {
            "customer_id": f"C{record_number:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": "",
            "status": random.choice(VALID_STATUSES),
            "signup_date": random_date(),
            "amount": round(random.uniform(-150, 2000), 2),
            "updated_at": (datetime.strptime(random_date(), "%Y-%m-%d") + timedelta(days=random.randint(1, 300))).strftime("%Y-%m-%d %H:%M:%S"),
            "region": random.choice(REGIONS),
            "source_system": random.choice(SOURCE_SYSTEMS),
        }
    if record_number % 19 == 0:
        return {
            "customer_id": f"C{(record_number % 8000) + 1:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": f"{random.choice(FIRST_NAMES).lower()}.{random.choice(LAST_NAMES).lower()}@{random.choice(DOMAINS)}",
            "status": random.choice(INVALID_STATUSES),
            "signup_date": random_date(),
            "amount": round(random.uniform(0, 9000), 2),
            "updated_at": (datetime.strptime(random_date(), "%Y-%m-%d") + timedelta(days=random.randint(1, 45))).strftime("%Y-%m-%d %H:%M:%S"),
            "region": random.choice(REGIONS),
            "source_system": random.choice(SOURCE_SYSTEMS),
        }

    customer_id = f"C{record_number:05d}"
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(DOMAINS)}"
    amount = round(random.uniform(-120, 4200), 2)
    signup_date = random_date(2021, 2025)
    status = random.choice(VALID_STATUSES)
    return {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "status": status,
        "signup_date": signup_date,
        "amount": amount,
        "updated_at": (datetime.strptime(signup_date, "%Y-%m-%d") + timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d %H:%M:%S"),
        "region": random.choice(REGIONS),
        "source_system": random.choice(SOURCE_SYSTEMS),
    }


def generate_dataset(rows: int = 100_000) -> Path:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "status",
        "signup_date",
        "amount",
        "updated_at",
        "region",
        "source_system",
    ]

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, rows + 1):
            row = rows_for_scenario(i)
            writer.writerow(row)

    print(f"Generated {rows} source rows to {OUTPUT_FILE}")
    return OUTPUT_FILE


if __name__ == "__main__":
    generate_dataset()
