CREATE TABLE Currency_Exchange.forex_batch_processing_2_day (
    event_id BIGINT NOT NULL,         -- Unique identifier for the event
    event_time BIGINT NOT NULL,      -- Epoch time in milliseconds
    ccy_couple VARCHAR(10) NOT NULL, -- Currency pair, e.g., EURUSD
    rate DECIMAL(15, 8) NOT NULL,    -- Exchange rate value with precision for FX rates
    PRIMARY KEY (event_id),          -- Primary key for uniqueness
    INDEX idx_event_time (event_time), -- Index to improve query performance on time-based filtering
    INDEX idx_ccy_couple (ccy_couple) -- Index to improve query performance on currency pair filtering
);


CREATE TABLE Currency_Exchange.active_rates_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    ccy_couple VARCHAR(10),
    current_rate DECIMAL(15, 8),
    percentage_change VARCHAR(10),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE Currency_Exchange.procedure_execution_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    procedure_name VARCHAR(50),
    rows_affected INT,
    execution_status VARCHAR(20),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
