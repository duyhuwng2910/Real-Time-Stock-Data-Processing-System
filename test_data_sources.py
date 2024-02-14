from ssi_fc_data import model
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from SSI import config
import vnstock

client = MarketDataClient(config)


def md_get_securities_list():
    req = model.securities('HNX', 1, 100)
    print(client.securities(config, req))


def md_get_securities_details():
    req = model.securities_details('HNX', 'ACB', 1, 100)
    print(client.securities_details(config, req))


# get market data message
def get_market_data(message):
    print(message)


# get error
def getError(error):
    print(error)


def main():
    # req = vnstock.listing_companies(live=True, source='SSI')
    # req = vnstock.company_overview('FRT')
    # req = vnstock.company_news(symbol='VHM', page_size=10, page=0)

    # df = vnstock.stock_historical_data(symbol='VHM',
    #                                    start_date="2010-01-01",
    #                                    end_date='2010-12-31',
    #                                    resolution='1D', type='stock', beautify=True, decor=False,
    #                                    source='TCBS')

    # df = vnstock.stock_intraday_data(symbol='VCB', page_size=500, investor_segment=True)
    #
    # print(df)

    # req = vnstock.price_depth('BID, VCB, VPB')

    df = vnstock.general_rating('VIC')

    print(df.dtypes)

    # print(req.dtypes)

    # selected_channel = input("Please select channel: ")
    # mm = MarketDataStream(config, MarketDataClient(config))
    # mm.start(get_market_data, getError, selected_channel)
    # message = None
    # test_list = []
    #
    # while message != "exit()":
    #     print("while")
    #
    #     try:
    #         message = input("Real time stock trading orders:\n")
    #         if message is not None and message != "" and message != "exit()":
    #             print("check")
    #             mm.swith_channel(message)
    #
    #         print("here")
    #
    #     except KeyboardInterrupt:
    #         print("Thoát khỏi vòng lặp!")
    #         break
    #
    # print(len(test_list))
    #
    # for message in test_list:
    #     print(message)


if __name__ == '__main__':
    main()
