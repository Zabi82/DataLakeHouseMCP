import os
from dotenv import load_dotenv

# Load environment variables once
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
FLINK_REST_URL = os.getenv("FLINK_REST_URL", "http://localhost:8081")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
# Use log file from $HOME directory
LOG_FILE = os.path.join(os.path.expanduser("~"), os.getenv("LOG_FILE", "lakehouse_mcp.log"))
