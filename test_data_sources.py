import threading
import pandas as pd
import datetime
import time
import os
from collections import OrderedDict

import mysql.connector
from sqlalchemy import create_engine, types

import vnstock_data
import vnstock
import sys

# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

print(vnstock_data.ssi.get_index_component(client, config, index='VN100', page=1, pageSize=100))