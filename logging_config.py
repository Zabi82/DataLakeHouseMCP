import logging
from env_config import LOG_FILE

# Centralized logger setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE)]  # Only file logging, no console
)

logger = logging.getLogger("lakehouse_mcp")
