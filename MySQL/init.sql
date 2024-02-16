USE demo;

DROP TABLE IF EXISTS companies_list_default;

CREATE TABLE companies_list_default (
    ticker VARCHAR(255) PRIMARY KEY,
    com_group_code VARCHAR(255),
    organ_name VARCHAR(255),
    organ_short_name VARCHAR(255),
    organ_type_code VARCHAR(255),
    com_type_code VARCHAR(255),
    icb_name VARCHAR(255),
    icb_name_path VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255),
    group_name VARCHAR(255),
    sub_group VARCHAR(255),
    icb_code BIGINT,
    VN30 BOOLEAN,
    VNMID BOOLEAN,
    VN100 BOOLEAN,
    VNSML BOOLEAN,
    VNALL BOOLEAN,
    HNX30 BOOLEAN,
    VNX50 BOOLEAN,
    VNXALL BOOLEAN,
    VNDIAMOND BOOLEAN,
    VNFINLEAD BOOLEAN,
    VNFINSELECT BOOLEAN,
    VNSI BOOLEAN,
    VNCOND BOOLEAN,
    VNCONS BOOLEAN,
    VNENE BOOLEAN,
    VNFIN BOOLEAN,
    VNHEAL BOOLEAN,
    VNIND BOOLEAN,
    VNIT BOOLEAN,
    VNMAT BOOLEAN,
    VNREAL BOOLEAN,
    VNUTI BOOLEAN
);

DROP TABLE IF EXISTS companies_list_live;

CREATE TABLE companies_list_live (
    organ_code VARCHAR(50),
    ticker VARCHAR(5) PRIMARY KEY,
    com_group_code VARCHAR(10),
    icb_code NVARCHAR(50),
    organ_type_code VARCHAR(5),
    com_type_code VARCHAR(10),
    organ_name VARCHAR(255),
    organ_short_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS companies_overview (
    ticker VARCHAR(255) PRIMARY KEY,
    exchange_name VARCHAR(255),
    industry VARCHAR(255),
    company_type VARCHAR(255),
    no_share_holders BIGINT,
    foreign_percent FLOAT,
    out_standing_share FLOAT,
    issue_share FLOAT,
    established_year VARCHAR(255),
    no_employees BIGINT,
    stock_rating FLOAT,
    delta_in_week FLOAT,
    delta_in_month FLOAT,
    delta_in_year FLOAT,
    short_name VARCHAR(255),
    industry_en VARCHAR(255),
    industry_id INT,
    industry_id_v2 VARCHAR(255),
    website VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stock_historical_data_one_min (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_three_mins (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_five_mins (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_fifteen_mins (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_thirty_mins (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_one_hour (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_historical_data_one_day (
    `time` DATETIME,
    `open` INTEGER,
    high INTEGER,
    low INTEGER,
    `close` INTEGER,
    volume BIGINT,
    ticker VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_intraday_data (
    ticker VARCHAR(10) PRIMARY KEY,
    `time` DATETIME,
    order_type VARCHAR(20),
    investor_type VARCHAR(20),
    volume BIGINT,
    average_price FLOAT,
    order_count INT,
    previous_price_change FLOAT
);

CREATE TABLE IF NOT EXISTS general_rating (
  ticker VARCHAR(255) PRIMARY KEY,
  stock_rating FLOAT,
  valuation FLOAT,
  financial_health FLOAT,
  business_model FLOAT,
  business_operation FLOAT,
  rs_rating FLOAT,
  ta_score FLOAT,
  highest_price FLOAT,
  lowest_price FLOAT,
  price_change_3m FLOAT,
  price_change_1y FLOAT,
  beta FLOAT,
  alpha FLOAT
);