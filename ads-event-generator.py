import random
import uuid
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import socket
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import io

# ========================
# Avro Schema Definition
# ========================
AVRO_SCHEMA_STRING = """
{
    "type": "record",
    "name": "AdEvent",
    "namespace": "netflix.ads",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "device_id", "type": "string"},
        {"name": "app_version", "type": "string"},
        {"name": "os_type", "type": "string"},
        {"name": "country_code", "type": "string"},
        {"name": "ip_address", "type": "string"},
        {"name": "ad_id", "type": "string"},
        {"name": "campaign_id", "type": "string"},
        {"name": "creative_id", "type": "string"},
        {"name": "advertiser_id", "type": "string"},
        {"name": "placement_id", "type": "string"},
        {"name": "bid_price", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "latency_ms", "type": "int"},
        {"name": "connection_type", "type": "string"},
        {"name": "is_skippable", "type": "boolean"},
        {"name": "skip_after_seconds", "type": "int"},
        {"name": "ad_format", "type": "string"},
        {"name": "content_id", "type": "string"},
        {"name": "content_genre", "type": "string"},
        {"name": "user_age", "type": "int"},
        {"name": "user_segment", "type": "string"},
        {"name": "device_model", "type": "string"},
        {"name": "pixel_ratio", "type": "string"},
        {"name": "viewability_score", "type": "double"},
        {"name": "audio_muted", "type": "boolean"},
        {"name": "player_state", "type": "string"},
        {"name": "is_pre_roll", "type": "boolean"},
        {"name": "is_mid_roll", "type": "boolean"},
        {"name": "is_post_roll", "type": "boolean"},
        {"name": "sdk_version", "type": "string"},
        {"name": "request_id", "type": "string"},
        {"name": "trace_id", "type": "string"},
        {"name": "network_carrier", "type": ["null", "string"]},
        {"name": "user_subscription_type", "type": "string"},
        {"name": "timezone", "type": "string"},
        {"name": "ad_duration", "type": "int"},
        {"name": "time_watched", "type": "int"},
        {"name": "ad_completion_rate", "type": "double"},
        {"name": "click_event_captured", "type": "boolean"},
        {"name": "click_timestamp", "type": "long"},
        {"name": "interaction_type", "type": ["null", "string"]},
        {"name": "referrer_url", "type": "string"},
        {"name": "campaign_objective", "type": "string"},
        {"name": "bidding_strategy", "type": "string"},
        {"name": "experiment_group_id", "type": "string"},
        {"name": "geo_location_lat_long", "type": "string"}
    ]
}
"""

# Load Avro schema
avro_schema = avro.schema.parse(AVRO_SCHEMA_STRING)

# ========================
# Configuration
# ========================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ad_events_avro"  # Different topic for Avro data

# Avro serializer function
def avro_serializer(data):
    writer = DatumWriter(avro_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()

# Create Kafka Producer with Avro serializer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=avro_serializer,
    compression_type="gzip",
    retries=3,
)

# ========================
# Helper functions
# ========================
def get_timestamp_ms():
    return int(time.time() * 1000)

def get_masked_ip():
    base = f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}"
    return f"{base}.xxx"

def get_random_user_segment():
    return random.choices(["Premium", "Standard", "Basic", "Trial"], weights=[0.4, 0.3, 0.2, 0.1])[0]

def get_geo_lat_long():
    lat = round(random.uniform(8.0, 34.0), 4)
    lon = round(random.uniform(68.0, 88.0), 4)
    return f"{lat},{lon}"

def generate_ad_event():
    # Basic identifiers
    event_id = str(uuid.uuid4())
    timestamp = get_timestamp_ms()
    user_id = f"USER_{random.randint(10000, 99999)}"
    session_id = f"SESS_{uuid.uuid4().hex[:8]}"
    device_id = f"DEV_{uuid.uuid4().hex[:12]}"
    
    # Device & network
    app_version = f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
    os_type = random.choice(["Android", "iOS", "tvOS", "FireOS"])
    country_code = random.choices(["IN", "US", "GB", "CA", "AU"], weights=[0.7, 0.1, 0.05, 0.05, 0.05])[0]
    ip_address = get_masked_ip()
    
    # Ad & campaign
    ad_id = f"AD_{random.randint(1000, 9999)}"
    campaign_id = f"CAMP_{random.randint(1, 20)}"
    creative_id = f"CREATIVE_{random.randint(5000, 5999)}"
    advertiser_id = f"ADV_{random.randint(100, 200)}"
    placement_id = f"PLACE_{random.randint(1, 10)}"
    
    # Bidding & pricing
    bid_price = round(random.uniform(0.1, 8.5), 2)
    currency = random.choices(["USD", "INR", "EUR"], weights=[0.6, 0.3, 0.1])[0]
    latency_ms = random.randint(10, 400)
    
    # Connection & playback
    connection_type = random.choices(["Wifi", "Cellular"], weights=[0.7, 0.3])[0]
    is_skippable = random.choice([True, False])
    skip_after_seconds = random.randint(5, 15) if is_skippable else 0
    
    ad_format = random.choices(["Video", "Banner"], weights=[0.95, 0.05])[0]
    content_id = f"MOVIE_{random.randint(1000, 9999)}"
    content_genre = random.choice(["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Romance", "Documentary"])
    
    # User details
    user_age = random.randint(18, 65)
    user_segment = get_random_user_segment()
    device_model = random.choice(["iPhone 14", "Pixel 7", "OnePlus 11", "Samsung S23", "Fire Stick 4K", "Chromecast"])
    pixel_ratio = random.choice(["1x", "1.5x", "2x", "3x"])
    viewability_score = round(random.uniform(0.3, 1.0), 2)
    
    # Player state & ad position
    audio_muted = random.choice([True, False])
    player_state = random.choices(["Playing", "Paused"], weights=[0.9, 0.1])[0]
    is_pre_roll = random.choice([True, False])
    is_mid_roll = random.choice([True, False]) if not is_pre_roll else False
    is_post_roll = random.choice([True, False]) if not (is_pre_roll or is_mid_roll) else False
    
    # Tech & tracing
    sdk_version = f"{random.randint(3,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
    request_id = str(uuid.uuid4())
    trace_id = f"TRACE_{uuid.uuid4().hex[:16]}"
    network_carrier = random.choice(["Airtel", "Jio", "Vi", "T-Mobile", "Verizon", None])
    user_subscription_type = random.choices(["Free", "Paid"], weights=[0.2, 0.8])[0]
    timezone = random.choice(["Asia/Kolkata", "America/New_York", "Europe/London", "Australia/Sydney"])
    
    # Ad performance metrics
    ad_duration = random.choice([15, 30, 45, 60])
    time_watched = random.randint(0, ad_duration)
    ad_completion_rate = round(time_watched / ad_duration, 2) if ad_duration > 0 else 0.0
    
    # Interaction & click
    click_event_captured = random.choice([True, False]) if time_watched > 3 else False
    click_timestamp = get_timestamp_ms() if click_event_captured else 0
    interaction_type = random.choice(["Tap", "Hover", None]) if click_event_captured else None
    
    # Campaign & experiment
    referrer_url = f"https://partner.com/ref/{random.randint(100,999)}"
    campaign_objective = random.choice(["Awareness", "Conversion"])
    bidding_strategy = random.choice(["CPM", "CPC"])
    experiment_group_id = random.choice(["control", "variant_A", "variant_B"])
    geo_location_lat_long = get_geo_lat_long()
    
    # Build final event dict (matches Avro schema)
    event = {
        "event_id": event_id,
        "timestamp": timestamp,
        "user_id": user_id,
        "session_id": session_id,
        "device_id": device_id,
        "app_version": app_version,
        "os_type": os_type,
        "country_code": country_code,
        "ip_address": ip_address,
        "ad_id": ad_id,
        "campaign_id": campaign_id,
        "creative_id": creative_id,
        "advertiser_id": advertiser_id,
        "placement_id": placement_id,
        "bid_price": bid_price,
        "currency": currency,
        "latency_ms": latency_ms,
        "connection_type": connection_type,
        "is_skippable": is_skippable,
        "skip_after_seconds": skip_after_seconds,
        "ad_format": ad_format,
        "content_id": content_id,
        "content_genre": content_genre,
        "user_age": user_age,
        "user_segment": user_segment,
        "device_model": device_model,
        "pixel_ratio": pixel_ratio,
        "viewability_score": viewability_score,
        "audio_muted": audio_muted,
        "player_state": player_state,
        "is_pre_roll": is_pre_roll,
        "is_mid_roll": is_mid_roll,
        "is_post_roll": is_post_roll,
        "sdk_version": sdk_version,
        "request_id": request_id,
        "trace_id": trace_id,
        "network_carrier": network_carrier,
        "user_subscription_type": user_subscription_type,
        "timezone": timezone,
        "ad_duration": ad_duration,
        "time_watched": time_watched,
        "ad_completion_rate": ad_completion_rate,
        "click_event_captured": click_event_captured,
        "click_timestamp": click_timestamp,
        "interaction_type": interaction_type,
        "referrer_url": referrer_url,
        "campaign_objective": campaign_objective,
        "bidding_strategy": bidding_strategy,
        "experiment_group_id": experiment_group_id,
        "geo_location_lat_long": geo_location_lat_long,
    }
    return event

# ========================
# Main streaming loop
# ========================
def produce_events(batch_size=100, sleep_sec=0.5):
    print(f"🚀 Producing Avro ad events to Kafka topic: {TOPIC_NAME}")
    print(f"Broker: {KAFKA_BROKER}")
    print(f"Format: Avro")
    
    while True:
        for _ in range(batch_size):
            event = generate_ad_event()
            
            # Send to Kafka (automatically serialized to Avro)
            future = producer.send(TOPIC_NAME, value=event)
            try:
                record_metadata = future.get(timeout=5)
                print(f"✅ Sent Avro: {event['event_id'][:8]}... @ offset {record_metadata.offset}")
            except Exception as e:
                print(f"❌ Failed to send: {e}")
        
        # Batch pause to simulate realistic flow
        time.sleep(sleep_sec)

if __name__ == "__main__":
    try:
        produce_events(batch_size=50, sleep_sec=1)
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped.")
    finally:
        producer.flush()
        producer.close()