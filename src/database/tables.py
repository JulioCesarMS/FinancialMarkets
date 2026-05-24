

# crea tabla de mercados
def table_markets():
    
    query = """   
        CREATE TABLE IF NOT EXISTS markets (
            market_id INT AUTO_INCREMENT PRIMARY KEY,
            market_name VARCHAR(100) NOT NULL UNIQUE,
            country VARCHAR(50),
            currency VARCHAR(10)
        );
    """
    return query


# crea tabla de compañias
def table_companies():
    
    query = """   
        CREATE TABLE IF NOT EXISTS companies (
            company_id INT AUTO_INCREMENT PRIMARY KEY,
            market_id INT NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            name VARCHAR(255) NOT NULL,
            sector_name VARCHAR(100) NOT NULL,
            sub_industry VARCHAR(100),
            date_added DATE,
            headquarters VARCHAR(255),
            cik VARCHAR(20),
            founded INT,
            FOREIGN KEY (market_id) REFERENCES markets(market_id),
            INDEX idx_market_date (market_id)
        );
    """
    return query

# crea tabla precios historicos
def tabla_precios(market):
    
    
    query = f"""
        CREATE TABLE IF NOT EXISTS stock_prices_{market} (
        price_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        company_id INT NOT NULL,
        date DATE NOT NULL,
        open_price DECIMAL(10,4),
        close_price DECIMAL(10,4),
        high_price DECIMAL(10,4),
        low_price DECIMAL(10,4),
        volume BIGINT,
        FOREIGN KEY (company_id) REFERENCES companies(company_id),
        INDEX idx_company_date (company_id, date)
    );
    """
    return query


