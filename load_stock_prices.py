import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
# web scrapping
import bs4 as bs
import requests
import lxml
from functools import reduce
# matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from ipysigma import Sigma
from pyvis.network import Network
import requests
from bs4 import BeautifulSoup
from io import StringIO
from dbconnection import MySQLDatabase
from utils import getSymbols, getData, get_last_date, get_marketid_simbols
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
sns.set_theme()



# conección a base de datos
db = MySQLDatabase("financialmarkets")


# lista de mercados a actualizar
mercados = ['S&P 500', 'NASDAQ']
# dataframe con compañias y id
company_map = db.execute_query("SELECT company_id, symbol FROM companies")
for mercado in mercados: 
    market_id, list_symbols = get_marketid_simbols(mercado, conn=db)
    print(f"Actualizando {mercado}: ")
    k = 1
    for symbol in list_symbols:
        start_date = get_last_date(symbol, conn=db)
        print(f"{k}) {symbol} desde {start_date}")
        data = yf.download(symbol, start=start_date, progress=False, auto_adjust=False)
        
        if data.empty:
            continue
        
        data = data.reset_index()
        data.columns = [x for x,y in data.columns]
        #data['Symbol'] = [symbol for x in data['Symbol']]
        data = data.rename(columns={
            'Date': 'date',
            'Open': 'open_price',
            'Close': 'close_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Volume': 'volume'
        })
        
        data['date'] = pd.to_datetime(data['date'], "%Y-%m-%d")
        # Interpolación lineal
        data = data.set_index('date').asfreq('D')
        #print(data.reset_index())
        data[['open_price','close_price','high_price','low_price','volume']] = \
            data[['open_price','close_price','high_price','low_price','volume']].interpolate(method='linear')
        data.loc[:,'symbol'] = [symbol for x in data['open_price']]
        data = data.reset_index()
        data = data.merge(company_map, left_on='symbol', right_on='symbol', how='left')
        data.loc[:,'market_id'] = [market_id for x in data['open_price']]
        # limpieza
        data_final = data[['company_id','market_id','date','open_price','close_price','high_price','low_price','volume']]
        data_final.loc[:,'company_id'] = data_final['company_id'].astype(int)
        data_final.loc[:,'market_id'] = data_final['market_id'].astype(int)
        data_final.loc[:,'date'] = pd.to_datetime(data_final['date'])
        data_final.loc[:,'open_price'] = data_final['open_price'].astype(float)
        data_final.loc[:,'close_price'] = data_final['close_price'].astype(float)
        data_final.loc[:,'high_price'] = data_final['high_price'].astype(float)
        data_final.loc[:,'low_price'] = data_final['low_price'].astype(float)
        data_final.loc[:,'volume'] = data_final['volume'].astype(int)
        # inserta datos a base 
        db.insert_to_db(data_final, tabla="stock_prices", batch_size=500)
        k += 1
    print("\n")

db.close()