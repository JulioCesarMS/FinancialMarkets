
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


# configuración base de datos
class DB:
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    NAME = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")

# directorio
BASE_DIR = Path(__file__).resolve().parents[1]

# tablas en base de datos
MARKETS = {
    "stock_prices_sp500": "S&P 500",
    "stock_prices_nasdaq": "NASDAQ",
    "stock_prices_ipcmx": "IPC MX",
    "stock_prices_dax": "DAX",
    "stock_prices_ftse100": "FTSE100",
    "stock_prices_forex": "FOREX"
}