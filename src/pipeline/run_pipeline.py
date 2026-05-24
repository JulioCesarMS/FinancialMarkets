from src.database.create_tables import CreateTables
from src.database.mysql_client import MySQLDatabase
from config.settings import MARKETS
from src.extraction.downloader import extract_stock_data, companies_dim, markets_dim
from src.extraction.extractor import companies, markets
from src.load.loader import load_stock_data
from src.transformation.transformer import transform_stock_data, transform_companies, transform_markets
from src.utils.getLastDate import get_last_date
from src.utils.getMarketid import get_marketid_simbols
import time


market_labels = [key.split("_")[-1] for key in MARKETS.keys()]

def run():
    start_time = time.time()
    # conexión a base de datos
    db = CreateTables("financialmarkets")
    # creación de tablas sino existen
    db.markets()
    db.companies()
    for market in market_labels:
        db.prices(market)
    # lectura de archivos
    df_companies = companies()
    df_markets = markets()
    # transformacion
    df_transformed_companies = transform_companies(df_companies)
    df_transformed_markets = transform_markets(df_markets)
    # si no hay datos carga info de compañias a la bd
    if companies_dim(db) == 0: 
        load_stock_data(db, df_transformed_companies, table_name="companies")
    # si no hay datos carga info de compañias a la bd
    if markets_dim(db) == 0: 
        load_stock_data(db, df_transformed_markets, table_name="markets")
    for table_name, mercado in MARKETS.items():
        # obtiene los simbolos de cada mercado
        market_id, list_companyid, list_symbols = get_marketid_simbols(mercado, conn=db)
        print(f"-----    Actualizando {mercado}  -------")
        for k, (company_id, symbol) in enumerate(zip(list_companyid, list_symbols), start=1):
            # obtiene ultima fecha de la tabla
            start_date = get_last_date(symbol, table_name=table_name, conn=db)
            print(f"{k}) {symbol} desde {start_date}")
            # Extrae
            raw_data = extract_stock_data(symbol, start_date)
            # Transforma
            transformed_data = transform_stock_data(raw_data, company_id, symbol)
            # Caega
            load_stock_data(db, transformed_data, table_name)
        print("\n")
    end_time = time.time()
    duration = int((end_time - start_time)//60)
    print(f"Tiempo: {duration} minutos")
    
    db.close()
    
    
    