import threading

from SSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

import time


def get_market_data(message: str):
    print(message)


def get_error(error: str):
    print(error)


def streaming(streams: MarketDataStream, ticker: str):
    ticker = 'B:' + ticker

    streams.start(get_market_data, get_error, ticker)

    while True:
        print(ticker)

        time.sleep(1)


def main():
    ticker_list = ['BID', 'CTG', 'TCB']

    thread_list = []

    for ticker in ticker_list:
        stream = MarketDataStream(config, MarketDataClient(config))
        
        thread = threading.Thread(target=streaming, args=(stream, ticker))

        thread_list.append(thread)

        thread.start()

    for thread in thread_list:
        thread.join()


main()
