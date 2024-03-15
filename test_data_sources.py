import threading
import pandas as pd
import datetime
import time
import os

import mysql.connector
from sqlalchemy import create_engine, types

import vnstock_data
import vnstock
from ssi_fc_data import fc_md_client, model
import sys

# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

connection = mysql.connector.connect(user='root',
                                     password='root',
                                     host='localhost',
                                     database='vietnam_stock')

cursor = connection.cursor()

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/vietnam_stock")

try:
    company_overview = vnstock.company_overview('GAS')

except Exception as e:

    print(f"Error while ingesting data:{e}")

company_overview.rename(columns={
'exchange': 'exchange_name',
'companyType': 'company_type',
'noShareholders': 'number_of_shareholders',
'foreignPercent': 'foreign_percent',
'outstandingShare': 'outstanding_share',
'issueShare': 'issue_share',
'establishedYear': 'established_year',
'noEmployees': 'number_of_employees',
'stockRating': 'stock_rating',
'deltaInWeek': 'delta_in_week',
'deltaInMonth': 'delta_in_month',
'deltaInYear': 'delta_in_year',
'shortName': 'short_name',
'industryEn': 'industry_en',
'industryID': 'industry_id',
'industryIDv2': 'industry_id_v2'
},
inplace=True)

try:
    company_overview.to_sql('companies_overview', con=engine, if_exists='append', index=False,
                    index_label='ticker',
                    dtype={
                    'ticker': types.VARCHAR(255),
                    'exchange_name': types.VARCHAR(255),
                    'industry': types.VARCHAR(255),
                    'company_type': types.VARCHAR(255),
                    'no_share_holders': types.BIGINT,
                    'foreign_percent': types.FLOAT,
                    'out_standing_share': types.FLOAT,
                    'issue_share': types.FLOAT,
                    'established_year': types.VARCHAR(255),
                    'no_employees': types.BIGINT,
                    'stock_rating': types.FLOAT,
                    'delta_in_week': types.FLOAT,
                    'delta_in_month': types.FLOAT,
                    'delta_in_year': types.FLOAT,
                    'short_name': types.VARCHAR(255),
                    'industry_en': types.VARCHAR(255),
                    'industry_id': types.INT,
                    'industry_id_v2': types.VARCHAR(255),
                    'website': types.VARCHAR(255)
                    })

    print(f"Insert companies overview data of completely!")

except Exception as e:
    print(f"Error here:{e}")