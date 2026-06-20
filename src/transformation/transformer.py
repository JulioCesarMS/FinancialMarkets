import pandas as pd


def transform_stock_data(data, company_id, symbol):
    if data.empty:
        return pd.DataFrame()
    data = data.reset_index()
    data.columns = [x for x, y in data.columns]
    data = data.rename(columns={
        'Date': 'date',
        'Open': 'open_price',
        'Close': 'close_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Volume': 'volume'
    })
    data['date'] = pd.to_datetime(data['date'])
    # frecuencia diaria
    data = data.set_index('date').asfreq('D')
    # interpolación
    cols = [
        'open_price',
        'close_price',
        'high_price',
        'low_price',
        'volume'
    ]
    data[cols] = data[cols].interpolate(method='linear')
    data.loc[:,'company_id'] = [company_id for x in data['open_price']]
    data.loc[:,'symbol'] = [symbol for x in data['open_price']]
    data = data.reset_index()
    # orden de columnas
    data_final = data[['company_id','date','open_price','close_price','high_price','low_price','volume']].copy()
    # tipos de datos
    data_final['company_id'] = data_final['company_id'].astype(int)
    data_final['date'] = pd.to_datetime(data_final['date'])
    data_final['open_price'] = data_final['open_price'].astype(float)
    data_final['close_price'] = data_final['close_price'].astype(float)
    data_final['high_price'] = data_final['high_price'].astype(float)
    data_final['low_price'] = data_final['low_price'].astype(float)
    data_final['volume'] = data_final['volume'].astype(int)
    return data_final


# formato de campos
def transform_companies(df): 
    df['company_id'] = df['company_id'].astype('int')
    df['market_id'] = df['market_id'].astype('int')
    df['symbol'] = df['symbol'].astype('str')
    df['name'] = df['name'].astype('str')
    df['sector_name'] = df['sector_name'].astype('str')
    df['sub_industry'] = df['sub_industry'].astype('str')
    df['date_added'] = pd.to_datetime(df['date_added'], errors="coerce").apply(lambda x:x.date() if pd.notnull(x) else None)
    df['headquarters'] = df['headquarters'].astype('str')
    df['cik'] = df['cik'].astype('str')
    df["founded"] = pd.to_numeric(df["founded"],errors="coerce").astype("int")
    return df


# formato campos
def transform_markets(df):
    df['market_id'] = df['market_id'].astype('int')
    df['market_name'] = df['market_name'].astype('str')
    df['country'] = df['country'].astype('str')
    df['currency'] = df['currency'].astype('str')
    return df