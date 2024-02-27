import random
import time

import finnhub
import pandas as pd
import datetime
import json
from kafka import KafkaProducer


def extract(symbol: str) -> dict:
    data = finnhub_client.quote(symbol)

    return data


def transform(symbol: str, data: dict):
    data['timestamps'] = datetime.datetime.utcfromtimestamp(int(data.pop('t'))).strftime('%Y-%m-%d %H:%M:%S')

    data['current_price'] = data.pop('c')
    data['change_value'] = data.pop('d')
    data['change_rate'] = data.pop('dp')
    data['open'] = data.pop('o')
    data['high'] = data.pop('h')
    data['low'] = data.pop('l')
    data['previous_price'] = data.pop('pc')

    data = {"symbol": symbol, **data}

    print(data)

    return data


def load(data: dict):
    message = producer.send('demo', data)

    # Chờ phản hồi
    record_metadata = message.get(5)  # Chờ tối đa 10 giây

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9094',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))  # Serialize json messages

    finnhub_client = finnhub.Client(api_key="cmm8tr9r01qqjtfo9h6gcmm8tr9r01qqjtfo9h70")

    top_tickers = [
        "MSFT", "AAPL", "GOOGL", "AMZN", "NVDA", "META", "BRK.B", "TSLA", "LLY", "AVGO",
        "V", "JPM", "TSM", "NVO", "UNH", "WMT", "MA", "JNJ", "XOM", "HD",
        "PG", "COST", "ORCL", "MRK", "ABBV", "ASML", "AMD", "ADBE", "CRM", "TM",
        "CVX", "KO", "BAC", "ACN", "PEP", "NVS", "MCD", "TMO", "NFLX", "CSCO",
        "AZN", "INTC", "ABT", "SHEL", "LIN", "SAP", "TMUS", "PDD", "WFC", "INTU"
    ]

    stock_df = pd.DataFrame(columns=["symbol", "timestamps", "current_price", "change_value", "change_rate",
                                     "open", "high", "low", "previous_price"])

    print("Starting sending messages...")

    time.sleep(1)

    for ticker in top_tickers:
        stock_data = extract(ticker)

        symbol_dict = transform(ticker, stock_data)

        load(symbol_dict)

        time.sleep(random.randint(0, 1))
