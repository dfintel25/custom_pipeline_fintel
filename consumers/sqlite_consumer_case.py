import os
import pathlib
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import time
import logging
from pathlib import Path

import utils.utils_config as config
from utils.utils_logger import logger

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "coffee_sales.sqlite"

#####################################
# Database Functions
#####################################

def init_db(db_path: pathlib.Path):
    """Initialize the SQLite database with coffee_sales table."""
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")
            cursor.execute("DROP TABLE IF EXISTS coffee_sales;")
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS coffee_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_of_day INTEGER,
                cash_type TEXT,
                money REAL,
                coffee_name TEXT,
                time_of_day TEXT,
                weekday TEXT,
                month_name TEXT,
                weekdaysort INTEGER,
                monthsort INTEGER,
                date TEXT,
                time TEXT,
                event_timestamp TEXT
            )
            """)
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """Insert a single processed message into the SQLite database."""
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO coffee_sales
            (hour_of_day, cash_type, money, coffee_name, time_of_day, weekday, month_name,
             weekdaysort, monthsort, date, time, event_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message["hour_of_day"],
                message["cash_type"],
                message["money"],
                message["coffee_name"],
                message["time_of_day"],
                message["weekday"],
                message["month_name"],
                message["weekdaysort"],
                message["monthsort"],
                message["date"],
                message["time"],
                message["event_timestamp"],
            ))
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """Delete a message from the SQLite database by its ID."""
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM coffee_sales WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")

#####################################
# Visualization Function
#####################################

def generate_reports(df: pd.DataFrame, output_prefix: str = "plots/"):
    os.makedirs(output_prefix, exist_ok=True)

    # Revenue by coffee type
    df.groupby("coffee_name")["money"].sum().plot(
        kind="bar", title="Revenue by Coffee Type"
    )
    plt.tight_layout()
    plt.savefig(f"{output_prefix}revenue_by_coffee.png")
    plt.close()

    # Sales trend by hour
    df.groupby("hour_of_day")["money"].sum().plot(
        kind="line", marker="o", title="Hourly Revenue Trend"
    )
    plt.tight_layout()
    plt.savefig(f"{output_prefix}hourly_revenue_trend.png")
    plt.close()

    # Payment method breakdown
    df.groupby("cash_type")["money"].sum().plot(
        kind="pie", autopct="%.1f%%", title="Payment Method Split"
    )
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(f"{output_prefix}payment_method_split.png")
    plt.close()

#####################################
# Kafka Consumer + Visualization
#####################################

def consume_and_visualize(kafka_consumer, db_path: pathlib.Path = DB_PATH):
    """Continuously consume messages and generate daily visualizations."""
    logging.info("Starting continuous SQLite consumer with daily visualization...")
    last_viz_date = None

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for message in kafka_consumer:
            record = message.value
            logging.info(f"Got record: {record}")

            # Insert into SQLite using existing function
            insert_message(record, db_path)

            # Generate visualization once per day
            current_date = record["date"]
            if last_viz_date != current_date:
                logging.info(f"Generating new visualization for {current_date}")
                df = pd.read_sql_query(
                    f"SELECT * FROM coffee_sales WHERE date='{current_date}'", conn
                )
                if not df.empty:
                    generate_reports(df, output_prefix=f"plots/{current_date}_")
                last_viz_date = current_date

            time.sleep(0.1)  # prevent CPU overuse

#####################################
# Main function for testing
#####################################

def main():
    logger.info("Starting db testing.")

    DATA_PATH: pathlib.Path = config.get_base_data_path()
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "hour_of_day": 10,
        "cash_type": "card",
        "money": 4.50,
        "coffee_name": "Latte",
        "time_of_day": "morning",
        "weekday": "Monday",
        "month_name": "September",
        "weekdaysort": 1,
        "monthsort": 9,
        "date": "2025-09-29",
        "time": "10:30:00",
        "event_timestamp": "2025-09-29 10:30:00",
    }

    insert_message(test_message, TEST_DB_PATH)

    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM coffee_sales WHERE coffee_name = ? AND cash_type = ?",
                (test_message["coffee_name"], test_message["cash_type"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()