
-- Tabla de mercados

CREATE TABLE IF NOT EXISTS markets (
    market_id INT AUTO_INCREMENT PRIMARY KEY,
    market_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50),
    currency VARCHAR(10)
);