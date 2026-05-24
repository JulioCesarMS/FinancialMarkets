from datetime import date, datetime, timedelta


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