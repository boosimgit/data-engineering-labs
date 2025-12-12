-- SCD2 Step 1:
-- Close old versions where tracked attributes changed
-- (city or tier in this example)

UPDATE dim_customer d
SET end_date  = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM stg_customer s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND (
       d.city <> s.city
    OR d.tier <> s.tier
  );

-- SCD2 Step 2:
-- Insert new records for:
--  - New customers
--  - Customers whose attributes changed

INSERT INTO dim_customer (
    customer_id,
    customer_name,
    city,
    tier,
    start_date,
    end_date,
    is_current
)
SELECT
    s.customer_id,
    s.customer_name,
    s.city,
    s.tier,
    CURRENT_DATE        AS start_date,
    DATE '9999-12-31'   AS end_date,
    TRUE                AS is_current
FROM stg_customer s
LEFT JOIN dim_customer d
    ON d.customer_id = s.customer_id
   AND d.is_current = TRUE
WHERE
    -- New customer: no current record
    d.customer_key IS NULL

    OR
    -- Attribute changed vs last current version
    d.city <> s.city
    OR d.tier <> s.tier;

commit;