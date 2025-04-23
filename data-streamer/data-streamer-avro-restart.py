
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroSchema

import os
import time
from datetime import datetime
import json
import pandas as pd
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env.local')
load_dotenv(dotenv_path)

CONFLUENT_SERVER = os.getenv("CONFLUENT_SERVER")
CONFLUENT_KEY = os.getenv("CONFLUENT_KEY")
CONFLUENT_SECRET = os.getenv("CONFLUENT_SECRET")
TOPIC_NAME = os.getenv("CONFLUENT_TOPIC")
CONFLUENT_SCHEMA_REGISTRY_URL = os.getenv("CONFLUENT_SCHEMA_REGISTRY")
CONFLUENT_SCHEMA_KEY = os.getenv("CONFLUENT_SCHEMA_KEY")
CONFLUENT_SCHEMA_SECRET = os.getenv("CONFLUENT_SCHEMA_SECRET")

# Define the Avro schema to match the CSV structure
avro_schema_str = """
{
  "type": "record",
  "name": "NetflixBrowseEvent",
  "namespace": "io.example.streaming",
  "doc": "Schema used to store Netflix Browse events.", 
  "fields": [
    {"name": "row_id", "type": "long"},
    {"name": "datetime", "type": "string", "doc": "TZ? UTC?"},
    {"name": "duration", "type": ["null", "double"], "doc":"How long in seconds a browse option is displayed."},
    {"name": "title", "type": ["null", "string"]},
    {"name": "genres", "type": ["null", "string"]},
    {"name": "release_date", "type": ["null", "string"]},
    {"name": "movie_id", "type": ["null", "string"]},
    {"name": "user_id", "type": ["null", "string"]}
  ]
}
"""

def send_to_kafka(producer, avro_serializer, events):
    try:
        for event in events:
            # Convert Timestamp to string and handle potential NaNs
            serializable_event = {}
            for key, value in event.items():
                if isinstance(value, pd.Timestamp):
                    serializable_event[key] = value.isoformat()
                elif pd.isna(value):
                    serializable_event[key] = None
                else:
                    serializable_event[key] = value

            producer.produce(
                topic=TOPIC_NAME,
                value=avro_serializer(serializable_event, ctx=SerializationContext(TOPIC_NAME, 'value'))
            )
        producer.flush()
        return True
    except ValueError as e:
        print(f"Error serializing message: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def create_kafka_producer():
    
    schema_registry_client = SchemaRegistryClient({
        'url': CONFLUENT_SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f"{CONFLUENT_SCHEMA_KEY}:{CONFLUENT_SCHEMA_SECRET}"
    })

    avro_serializer = AvroSerializer(
        schema_str=avro_schema_str,
        schema_registry_client=schema_registry_client,
        to_dict=lambda obj, ctx: obj
    )

    # Required connection configs for Kafka producer, consumer, and admin
    config = {
        'bootstrap.servers': CONFLUENT_SERVER,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': CONFLUENT_KEY,
        'sasl.password': CONFLUENT_SECRET
    }

    producer = Producer(config)

    return producer, avro_serializer

def stream_events(csv_filepath="your_data.csv", timestamp_column="datetime", speedup_factor=1.0, row_id_column_name="row_id", timestamp_file="data_time.txt"):
    try:
        producer, avro_serializer = create_kafka_producer()

        df = pd.read_csv(csv_filepath) # Read CSV without setting index initially

        # Check if the first column has a default name (like 'Unnamed: 0') or no name
        first_column = df.columns[0]
        if 'Unnamed' in first_column or first_column == '':
            df.rename(columns={first_column: row_id_column_name}, inplace=True)
        elif row_id_column_name not in df.columns:
            # If the first column has a name but it's not the expected row_id_column_name
            # and 'row_id' isn't already a column, we'll rename the first one.
            print(f"Warning: The first column '{first_column}' is being renamed to '{row_id_column_name}'.")
            df.rename(columns={first_column: row_id_column_name}, inplace=True)
        
        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in the CSV file.")

        try:
            df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        except Exception as e:
            raise ValueError(f"Error converting timestamp column '{timestamp_column}' to datetime: {e}")

        df_sorted = df.sort_values(by=timestamp_column)

        if df_sorted.empty:
            print("No data to stream.")
            return

        last_processed_timestamp = None
        try:
            with open(timestamp_file, 'r') as f:
                last_processed_timestamp_str = f.readline().strip()
                if last_processed_timestamp_str:
                    last_processed_timestamp = pd.to_datetime(last_processed_timestamp_str)
                    print(f"Resuming from timestamp: {last_processed_timestamp}")
        except FileNotFoundError:
            print(f"Timestamp file '{timestamp_file}' not found. Starting from the beginning.")
        except Exception as e:
            print(f"Error reading timestamp file: {e}. Starting from the beginning.")

        start_time = None
        previous_time = None

        print("Streaming events (in Avro format)...")
        for index, row in df_sorted.iterrows():
            current_time = row[timestamp_column]

            if last_processed_timestamp is not None and current_time <= last_processed_timestamp:
                continue  # Skip events that have already been processed

            event = row.to_dict()

            if start_time is None:
                start_time = current_time
                previous_time = current_time
                print(f"Avro Event: {event}")
                time.sleep(0)
                continue

            time_difference = (current_time - previous_time).total_seconds()
            delay = time_difference / speedup_factor

            if delay > 0:
                time.sleep(delay)

            events = [event]
            print(f"Avro Event: {event}")
            send_to_kafka(producer, avro_serializer, events)

            previous_time = current_time

        print("Avro streaming complete.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_filepath}'")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    csv_file = './input/vodclickstream_uk_movies_03_2019.csv'
    timestamp_col = "datetime"
    speedup = 150000
    timestamp_file = "data_time.txt"  # Define the name of your timestamp file


    stream_events(csv_filepath=csv_file, timestamp_column=timestamp_col, speedup_factor=speedup, timestamp_file=timestamp_file)
