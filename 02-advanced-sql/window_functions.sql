-- Number each sale for each customer by time
-- What was the first sale of each customer?

with tab as (
select sale_id,customer_id,sale_date,amount,
row_number()over(partition by customer_id order by sale_date) as sales_seq_for_customer 
from stg_sales order by customer_id,sale_date)
select * from tab where sales_seq_for_customer=1;

-- cumulative spend per customer over time

with tab2 as (
select sale_id,customer_id,sale_date,amount,
sum(amount)over(partition by customer_id order by sale_date) as cumulative_spend
from stg_sales order by customer_id,sale_date)
select * from tab2;

-- moving average of sales amount over last 3 sales per customer

with tab3 as (
select sale_id,customer_id,sale_date,amount,
avg(amount)over(partition by customer_id order by sale_date
rows between 2 preceding and current row) as moving_avg_3_sales
from stg_sales order by customer_id,sale_date)
select * from tab3;

-- rank customers by total spend

with total_spend as (
select customer_id,sum(amount)as total_amount
from stg_sales
group by customer_id),
ranked as (
select customer_id,total_amount,
rank()over(order by total_amount desc) as spend_rank
from total_spend)
select * from ranked order by spend_rank;   

-- percent rank of sales amounts

with percent_ranked as (
select sale_id,customer_id,sale_date,amount,
percent_rank()over(order by amount) as amount_percent_rank
from stg_sales)
select * from percent_ranked order by amount;   


-- Look at previous values 

with tab as (
select customer_id,customer_name,city,tier,start_date,end_date,
lag(city) over ( partition by customer_id order by start_date) as prev_city,
lag(tier) over(partition by customer_id order by start_date) as prev_tier from dim_customer dc )
select * from tab where city<>prev_city or tier<>prev_tier;


-- Rank and Dense Rank

select * from fact_sales t ;

select customer_key,sale_id,amount, 
rank()over(partition by customer_key order by amount desc) as sale_rank_customer,
dense_rank() over ( partition by customer_key order by amount desc ) as sale_dense_rank_customer 
from fact_sales t ;

-- Ntile

SELECT
    customer_key,
    amount,
    NTILE(2) OVER (
        ORDER BY amount
    ) AS spend_quartile
FROM fact_sales;

-- first value and last value 
-- for last value - use rows between unbounded preceeding and unbounded following

select customer_key,sale_date,amount,
first_value(amount)over ( partition by customer_key order by sale_date) as first_purchase_amount ,
last_value(amount)over ( partition by customer_key order by sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_purchase_amount 
from fact_sales order by customer_key, sale_date;

-- 3 day working avgs.
select customer_key,sale_id,amount, 
avg(amount)over(partition by customer_key order by sale_date rows between 2 preceding and current row ) as moving_avg
from fact_sales order by customer_key, sale_date;