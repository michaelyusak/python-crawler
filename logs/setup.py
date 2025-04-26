import logging
import os
from datetime import datetime

def setup_logger():
    now = datetime.now()
    LOG_FILE = "logs/" + now.strftime("%Y%m%d-%H:%M:%S") + ".log"

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
