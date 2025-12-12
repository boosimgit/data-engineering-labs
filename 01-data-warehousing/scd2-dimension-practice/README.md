# SCD2 Dimension Practice (PostgreSQL)

This folder contains hands-on exercises for implementing Slowly Changing Dimension Type 2 (SCD2) in PostgreSQL.

## What is covered:
- Creating a dimension table with surrogate keys
- Loading initial dimension data
- Creating a staging table (simulated source)
- Detecting changed vs unchanged data
- Closing old records (end_date updates)
- Inserting new versioned records
- Maintaining `start_date`, `end_date`, `is_current`

## How to run:

1. Start PostgreSQL:
brew services start postgresql

2. Open the database:
psql de_lab


3. Run table setup:


\i setup_tables.sql


4. Run SCD2 logic:


\i scd2_practice.sql


5. Validate results:


SELECT * FROM dim_customer ORDER BY customer_id, start_date;


This is the foundation for more advanced Data Engineering pipelines using PySpark and AWS