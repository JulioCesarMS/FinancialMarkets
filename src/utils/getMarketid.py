import pandas as pd
import numpy as np



# get marketid    
def get_marketid_simbols(market, conn):
    db = conn
    query = f"""  
        SELECT 
            m.market_id,
            m.market_name,
            c.company_id,
            c.symbol,
            c.name AS company_name
        FROM markets AS m
        JOIN companies AS c ON m.market_id = c.market_id
        WHERE m.market_name = '{market}';
    """

    result = db.execute_query(query)
    marketid = result['market_id'].unique()[0]
    symbols = result['symbol'].to_list()
    companyid = result['company_id'].to_list()
    return marketid, companyid, symbols