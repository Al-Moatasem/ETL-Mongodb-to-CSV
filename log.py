import logging
from datetime import datetime
import os
import json

with open('config.json', 'r') as cnfg_file:
    config = json.load(cnfg_file)
    logs = config['logging']

if not os.path.exists(logs['logging_path']):
    os.makedirs(logs['logging_path'])

now = datetime.now()
date = now.strftime(logs['log_file_date_prefix'])

logging.basicConfig(
    filename=fr"logs\{date}.log",
    level=logging.DEBUG,
    format="%(asctime)s || %(message)s",
    datefmt="%Y-%b-%d %H:%M:%S",
)


def log_msg(msg):
    logging.info(msg)
    print(msg)