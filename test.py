# import ssi_fc_data
import time

from SSI import config
import json
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient


# get market data message
def get_market_data(message):
    print(message)


# get error
def get_error(error):
    print(error)


# main function
def main():
    selected_channel = input("Please select channel: ")

    mm = MarketDataStream(config, MarketDataClient(config))

    mm.start(get_market_data, get_error, selected_channel)

    while True:
        print("after")

        time.sleep(1)


main()
