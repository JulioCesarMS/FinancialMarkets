
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




# Función para obtener los símbolos
def get_symbols(url):
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