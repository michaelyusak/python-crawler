import logging
import os
from datetime import datetime

def setup_logger():
    handler = [
        logging.StreamHandler()
    ]

    is_enabled = os.getenv("IS_LOG_ENABLED").lower() == "true"

    if is_enabled:
        now = datetime.now()
        LOG_FILE = "logs/" + now.strftime("%Y%m%d-%H:%M:%S") + ".log"

        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        handler.append(logging.FileHandler(LOG_FILE))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=handler
    )
