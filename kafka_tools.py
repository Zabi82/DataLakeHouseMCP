from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.errors import KafkaError
from fastavro import schemaless_reader
import requests
import io
import json
from env_config import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL
from logging_config import logger

def get_kafka_topics() -> dict:
    """
    Returns the list of topics from the local Kafka cluster using kafka-python.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topics = admin.list_topics()
        logger.info(f"Kafka topics fetched: {topics}")
        return {"topics": topics}
    except KafkaError as e:
        logger.error(f"Kafka error fetching topics: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"General error fetching topics: {e}")
        return {"error": str(e)}

def _sanitize_topic_name(topic: str) -> str:
    """
    Allow only alphanumeric, underscores, dashes, and dots in topic names.
    """
    import re
    if not re.match(r'^[a-zA-Z0-9_.-]+$', topic):
        raise ValueError(f"Invalid topic name: {topic}")
    return topic

def peek_kafka_topic(topic: str, max_messages: int = 10) -> dict:
    """
    Gets the latest max_messages from a given Kafka topic.
    Tries Avro deserialization if schema registry is available, otherwise returns JSON or plain text.
    Returns message type and decode errors for each message.
    """
    import base64
    try:
        topic = _sanitize_topic_name(topic)
    except Exception as e:
        logger.error(f"Invalid topic name '{topic}': {e}")
        return {"error": str(e)}
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
            consumer_timeout_ms=2000
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            logger.warning(f"No partitions found for topic {topic}")
            return {"error": f"No partitions found for topic {topic}"}
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(topic_partitions)
        for tp in topic_partitions:
            start_offset = max(end_offsets[tp] - max_messages, 0)
            consumer.seek(tp, start_offset)
        messages = []
        schema_registry_url = SCHEMA_REGISTRY_URL
        for msg in consumer:
            value = msg.value
            msg_info = {"type": None, "value": None, "decode_error": None}
            if value and value[:1] == b'\x00':
                schema_id = int.from_bytes(value[1:5], byteorder='big')
                try:
                    schema_resp = requests.get(f"{schema_registry_url}/schemas/ids/{schema_id}")
                    schema_resp.raise_for_status()
                    schema = schema_resp.json()["schema"]
                    schema = json.loads(schema)
                    avro_payload = io.BytesIO(value[5:])
                    decoded = schemaless_reader(avro_payload, schema)
                    msg_info["type"] = "avro"
                    msg_info["value"] = decoded
                except Exception as avro_err:
                    logger.error(f"Avro decode error for topic '{topic}': {avro_err}")
                    msg_info["type"] = "avro"
                    msg_info["value"] = base64.b64encode(value).decode("utf-8")
                    msg_info["decode_error"] = "Avro decode error. Message could not be decoded."
            else:
                try:
                    decoded_json = json.loads(value)
                    msg_info["type"] = "json"
                    msg_info["value"] = decoded_json
                except Exception as json_err:
                    try:
                        decoded_text = value.decode("utf-8")
                        msg_info["type"] = "text"
                        msg_info["value"] = decoded_text
                    except Exception as text_err:
                        msg_info["type"] = "binary"
                        msg_info["value"] = base64.b64encode(value).decode("utf-8")
                        msg_info["decode_error"] = f"JSON error: {json_err}; Text error: {text_err}"
            messages.append(msg_info)
            if len(messages) >= max_messages:
                break
        consumer.close()
        logger.info(f"Peeked {len(messages)} messages from topic '{topic}'")
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Error peeking topic '{topic}': {e}")
        return {"error": str(e)}
