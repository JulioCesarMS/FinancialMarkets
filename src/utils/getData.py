import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
# web scrapping
import bs4 as bs
import requests
import lxml
from functools import reduce
import requests
from bs4 import BeautifulSoup
from io import StringIO



def get_data(symbols, start_date, end_date):
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