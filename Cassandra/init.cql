CREATE KEYSPACE vietnam_stock WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1':'1', 'DC2':'1'};

USE vietnam_stock;

DROP TABLE IF EXISTS real_time_stock_trading_data;

CREATE TABLE real_time_stock_trading_data (
    id int,
    trading_time timestamp,
    ticker text,
    open int,
    high int,
    low int,
    close int,
    volume int,
    PRIMARY KEY (ticker, trading_time, id)
);

TRUNCATE TABLE real_time_stock_trading_data;

DROP TABLE IF EXISTS aggregated_stock_trading_data;

CREATE TABLE aggregated_stock_trading_data (
    start_time timestamp,
    end_time timestamp,
    ticker text,
    close int,
    high int,
    low int,
    open int,
    volume int,
    PRIMARY KEY (ticker, start_time)
);

TRUNCATE TABLE aggregated_stock_trading_data;

DROP TABLE IF EXISTS vn30_ticker_list;

CREATE TABLE vn30_ticker_list (
    ticker text,
    PRIMARY KEY (ticker)
);

TRUNCATE TABLE vn30_ticker_list;

DROP TABLE IF EXISTS intraday_data;

CREATE TABLE intraday_data (
    trading_time timestamp,
    ticker text,
    price int,
    volume int,
    PRIMARY KEY (ticker, trading_time)
);

DROP TABLE IF EXISTS stock_price_one_min;

CREATE TABLE stock_price_one_min (
    trading_time timestamp,
    ticker text,
    price int,
    volume int,
    PRIMARY KEY (ticker, trading_time)
)

TRUNCATE TABLE stock_price_one_min;

