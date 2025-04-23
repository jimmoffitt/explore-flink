import pandas as pd
import time
import os
import json
from confluent_kafka import Producer, KafkaError
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env.local')
load_dotenv(dotenv_path)

CONFLUENT_SERVER = os.getenv("CONFLUENT_SERVER")
CONFLUENT_KEY = os.getenv("CONFLUENT_KEY")
CONFLUENT_SECRET = os.getenv("CONFLUENT_SECRET")
TOPIC_NAME = os.getenv("CONFLUENT_TOPIC")

def send_to_kafka(producer, event): # Changed 'events' to 'event' - we're sending one at a time
    try:
        # Convert Timestamp objects to ISO format
        serializable_event = {}
        for key, value in event.items():
            if isinstance(value, pd.Timestamp):
                serializable_event[key] = value.isoformat()
            else:
                serializable_event[key] = value

        # Serialize the dictionary to a JSON string
        json_payload = json.dumps(serializable_event) # <--- Make sure you're using serializable_event here

        producer.produce(TOPIC_NAME, value=json_payload.encode('utf-8')) # Encode to bytes
        producer.flush()
        return True
    except KafkaError as error:
        print(f"Error sending message to Kafka: {error}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def create_kafka_producer():
    # Required connection configs for Kafka producer, consumer, and admin
    config = {
        'bootstrap.servers': CONFLUENT_SERVER,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': CONFLUENT_KEY,
        'sasl.password': CONFLUENT_SECRET
    }

    return Producer(config)

def stream_events(csv_filepath="your_data.csv", timestamp_column="timestamp", speedup_factor=1.0):
    """
    Streams 'event' data from a CSV file, sorted chronologically, with a configurable time speedup.

    Args:
        csv_filepath (str, optional): The path to the CSV file. Defaults to "your_data.csv".
        timestamp_column (str, optional): The name of the timestamp column. Defaults to "timestamp".
        speedup_factor (float, optional): The factor by which to speed up the stream.
                                         A value of 1.0 means real-time, >1.0 means faster, <1.0 means slower.
                                         Defaults to 1.0.
    """
    try:

        producer = create_kafka_producer()

        # Load the CSV data, providing column names
        column_names = ["row_id", "datetime", "duration", "title", "genres", "release_date", "movie_id", "user_id"]
        df = pd.read_csv(csv_filepath, header=0, names=column_names) # header=0 indicates the first row is the header

        # Ensure the timestamp column exists
        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in the CSV file.")

        # Try to convert the timestamp column to datetime objects
        try:
            df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        except Exception as e:
            raise ValueError(f"Error converting timestamp column '{timestamp_column}' to datetime: {e}")

        # Sort the DataFrame by the timestamp column in ascending order
        df_sorted = df.sort_values(by=timestamp_column)

        if df_sorted.empty:
            print("No data to stream.")
            return

        start_time = None
        previous_time = None

        print("Streaming events (as JSON objects)...")
        for index, row in df_sorted.iterrows():
            current_time = row[timestamp_column]
            event = row.to_dict()  # Convert the row to a dictionary

            if start_time is None:
                start_time = current_time
                previous_time = current_time
                print(f"Sending event: {event}") # Print the JSON object
                time.sleep(0)  # No initial delay
                continue

            time_difference = (current_time - previous_time).total_seconds()
            delay = time_difference / speedup_factor

            if delay > 0:
                time.sleep(delay)

            # Send the single event (as a dictionary) to the Kafka producer
            print(f"Sending event: {event}") # Print the JSON object
            send_to_kafka(producer, event)

            previous_time = current_time

        print("Streaming complete.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_filepath}'")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":

    #csv_file = './input/vodclickstream_uk_movies_03.csv'
    csv_file = './input/vodclickstream_uk_movies_03_2017.csv'
    timestamp_col = "datetime"
    speedup = 100000

    stream_events(csv_filepath=csv_file, timestamp_column=timestamp_col, speedup_factor=speedup)
