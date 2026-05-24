CREATE TABLE IF NOT EXISTS stock_prices_ipcmx (
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