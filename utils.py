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
sns.set_theme()




# Función para obtener los símbolos
def getSymbols(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        raise ValueError(f"Error al obtener la página: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")

    # Buscar cualquier tabla con clase 'wikitable'
    table = soup.find("table", {"class": lambda x: x and "wikitable" in x})
    if table is None:
        raise ValueError("No se encontró ninguna tabla con clase 'wikitable'")

    # Convertir tabla a DataFrame
    table = pd.read_html(StringIO(str(table)))[0]

    return table


def getData(symbols, start_date, end_date):
    stock_data = {}

    for symbol in symbols:
        # Validar que el símbolo sea string y no esté vacío
        if not isinstance(symbol, str) or symbol.strip() == '' or symbol.lower() == 'nan':
            print(f"[Aviso] Símbolo inválido ignorado: {symbol}")
            continue

        try:
            symb = yf.Ticker(symbol)
            hist = symb.history(start=start_date, end=end_date)

            if hist.empty:
                print(f"[Aviso] No se encontraron datos para {symbol}")
                continue

            stock_data[symbol] = hist['Close']

        except Exception as e:
            print(f"[Error] No se pudo obtener datos para {symbol}: {e}")

    if not stock_data:
        raise ValueError("No se pudo recuperar información para ningún símbolo.")

    df = pd.DataFrame(stock_data)
    return df


# get last date
def get_last_date(symbol, table_name, conn):
    db = conn
    query = f"""
        SELECT MAX(date) as last_date
        FROM {table_name} sp
        JOIN companies c ON sp.company_id = c.company_id
        WHERE c.symbol = '{symbol}';
    """
    result = db.execute_query(query)
    try: 
        start_date = (result['last_date'][0] + timedelta(days=1)).strftime('%Y-%m-%d')
    except:
        start_date = "2000-01-01"
    
    return start_date
    
# get marketid    
def get_marketid_simbols(market, conn):
    db = conn
    query = f"""  
        SELECT 
            m.market_id,
            m.market_name,
            c.symbol,
            c.name AS company_name
        FROM markets AS m
        JOIN market_companies AS mc ON m.market_id = mc.market_id
        JOIN companies AS c ON mc.company_id = c.company_id
        WHERE m.market_name = '{market}';
    """

    result = db.execute_query(query)
    marketid = result['market_id'].unique()[0]
    symbols = result['symbol'].to_list()
    
    return marketid, symbols