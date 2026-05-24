import pandas as pd
from config.settings import BASE_DIR



def companies():  
    DATA_PATH = (BASE_DIR / "data" / "raw" / "companies.xlsx")
    
    df = pd.read_excel(DATA_PATH)
    return df

def markets():  
    DATA_PATH = (BASE_DIR / "data" / "raw" / "markets.xlsx")
    df = pd.read_excel(DATA_PATH)
    return df



def  companies_dim_():
    query = """ 
    SELECT * 
    FROM companies
    """
    return query

def  markets_dim_():
    query = """ 
    SELECT * 
    FROM markets
    """
    return query
    