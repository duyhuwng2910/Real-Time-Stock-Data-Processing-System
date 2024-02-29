from vnstock_data.ssi import *

import sys

sys.path.append(r'SSI')

import config

client = fc_md_client.MarketDataClient(config)

securities_list(market='HOSE', size=1000, page=1, client=client, config=config)
