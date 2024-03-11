CREATE KEYSPACE vietnam_stock WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1':'1', 'DC2':'1'};

USE vietnam_stock;

CREATE TABLE real_time_stock_trading_data (
    trading_time timestamp,
    ticker text,
    open int,
    high int,
    low int,
    close int,
    volume int,
    PRIMARY KEY (ticker, trading_time)
);

TRUNCATE TABLE real_time_stock_trading_data;