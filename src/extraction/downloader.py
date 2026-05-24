import yfinance as yf
from src.extraction.extractor import companies_dim_, markets_dim_
import logging
import pandas as pd
import logging
logging.getLogger("yfinance").setLevel(logging.CRITICAL)



def extract_stock_data(symbol, start_date):

    try:
        # descarga
        data = yf.download(
            symbol,
            start=start_date,
            progress=False,
            auto_adjust=False
        )
        # validar datos vacíos
        if data.empty:
            print(f" Sin datos para: {symbol}")
            return pd.DataFrame()
        return data

    except Exception as e:
        print(
            f"Error descargando "
            f"{symbol}: {e}"
        )

        
def companies_dim(db):
    query = companies_dim_()
    df = db.execute_query(query)
    
    return df.shape[0]

def markets_dim(db):
    query = markets_dim_()
    df = db.execute_query(query)
    
    return df.shape[0]