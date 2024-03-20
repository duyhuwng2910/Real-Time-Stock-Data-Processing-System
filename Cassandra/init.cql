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

DROP TABLE IF EXISTS real_time_stock_trading_data_one_min;

CREATE TABLE real_time_stock_trading_data_one_min (
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

TRUNCATE TABLE real_time_stock_trading_data_one_min;