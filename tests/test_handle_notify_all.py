import os
import json
from app import handle_notify_all

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

if __name__ == '__main__':
    dest_dirname = "../data"
    if not os.path.exists(dest_dirname):
        os.makedirs(dest_dirname)
    with open("../tests/notify-all-1.json", "r") as f:
        notify_all_message = json.load(f)
        handle_notify_all(notify_all_message, dest_dirname)
