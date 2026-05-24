
-- Tabla de empresascompanies

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