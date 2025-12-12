-- Drop old tables if they exist (for reruns)
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS stg_customer;

-- Dimension table with surrogate key (customer_key)
CREATE TABLE dim_customer (
    customer_key   SERIAL PRIMARY KEY,      -- surrogate key
    customer_id    VARCHAR(50) NOT NULL,    -- business key
    customer_name  VARCHAR(200),
    city           VARCHAR(100),
    tier           VARCHAR(50),
    start_date     DATE NOT NULL,
    end_date       DATE NOT NULL,
    is_current     BOOLEAN NOT NULL
);

-- Initial snapshot of customers
INSERT INTO dim_customer (customer_id, customer_name, city, tier, start_date, end_date, is_current)
VALUES
('C001', 'Alice Kumar', 'Dallas',  'Silver', '2023-01-01', '9999-12-31', TRUE),
('C002', 'Bob Singh',   'Austin',  'Gold',   '2023-01-01', '9999-12-31', TRUE),
('C003', 'John Doe',    'Houston', 'Silver', '2023-01-01', '9999-12-31', TRUE);

-- Staging table simulating latest snapshot from source system
CREATE TABLE stg_customer (
    customer_id    VARCHAR(50) PRIMARY KEY,
    customer_name  VARCHAR(200),
    city           VARCHAR(100),
    tier           VARCHAR(50)
);

-- New snapshot:
-- - C001: city changed (Dallas -> Austin)
-- - C002: unchanged
-- - C003: tier changed (Silver -> Gold)
-- - C004: brand new customer
INSERT INTO stg_customer (customer_id, customer_name, city, tier)
VALUES
('C001', 'Alice Kumar', 'Austin', 'Silver'),
('C002', 'Bob Singh',   'Austin', 'Gold'),
('C003', 'John Doe',    'Houston','Gold'),
('C004', 'New User',    'Dallas', 'Bronze');

commit;