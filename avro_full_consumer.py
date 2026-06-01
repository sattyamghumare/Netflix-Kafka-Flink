# Save as avro_full_consumer.py
from kafka import KafkaConsumer, TopicPartition
import avro.schema
from avro.io import DatumReader, BinaryDecoder
import io
from datetime import datetime

# Your complete Avro schema from producer
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

# Load schema
schema = avro.schema.parse(AVRO_SCHEMA_STRING)
reader = DatumReader(schema)

# Kafka consumer config
TOPIC = 'ad_events_avro'
BOOTSTRAP_SERVERS = 'localhost:9092'

print("=" * 80)
print("🎬 Avro Consumer for Netflix Ad Events")
print("=" * 80)
print(f"Topic: {TOPIC}")
print(f"Broker: {BOOTSTRAP_SERVERS}")
print("=" * 80)

# Create consumer starting from latest messages
consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    enable_auto_commit=True
)
tp = TopicPartition(TOPIC, 0)
consumer.assign([tp])
consumer.seek_to_end(tp)  # Start from latest messages only

print("\n✅ Connected! Waiting for new messages...")
print("Press Ctrl+C to stop\n")

count = 0

try:
    for msg in consumer:
        # Decode Avro message
        bytes_reader = io.BytesIO(msg.value)
        decoder = BinaryDecoder(bytes_reader)
        event = reader.read(decoder)
        
        count += 1
        timestamp = datetime.fromtimestamp(event.get('timestamp', 0) / 1000)
        
        print(f"\n{'='*80}")
        print(f"📨 MESSAGE #{count}")
        print(f"{'='*80}")
        print(f"  Event ID:      {event.get('event_id')}")
        print(f"  Timestamp:     {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  User ID:       {event.get('user_id')}")
        print(f"  Campaign ID:   {event.get('campaign_id')}")
        print(f"  Bid Price:     ${event.get('bid_price')}")
        print(f"  Completion:    {event.get('ad_completion_rate', 0)*100:.1f}%")
        print(f"  Click Captured: {'✅ YES' if event.get('click_event_captured') else '❌ NO'}")
        print(f"  User Segment:  {event.get('user_segment')}")
        print(f"  Country:       {event.get('country_code')}")
        print(f"  OS Type:       {event.get('os_type')}")
        print(f"  Ad Duration:   {event.get('ad_duration')}s")
        print(f"  Time Watched:  {event.get('time_watched')}s")
        
except KeyboardInterrupt:
    print(f"\n\n✅ Stopped. Received {count} new messages")
