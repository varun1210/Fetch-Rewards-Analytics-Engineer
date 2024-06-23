-- Question 1 (What are the top 5 brands by receipts scanned for most recent month?)
with month_year as (
    select distinct
	extract(month from date_scanned) as month,
	extract(year from date_scanned) as year
    from src_receipts 
),
months_ago as (
    select month, year, 
	row_number() over(order by year desc, month desc) as months_ago
    from month_year
),
recent as (
    select month, year 
    from months_ago where months_ago = 1
),
receipts as (
    select receipt_id
    from src_receipts r
    inner join recent rec
    on extract(year from r.date_scanned) = rec.year
    and extract(month from r.date_scanned) = rec.month
),
items as (
    select i.brand_code
    from src_items i
    inner join receipts r
    on i.receipt_id = r.receipt_id
    where i.src_file = 'receipts.json'
),
brands as (
    select brand_name, brand_code
    from src_brands
)
select i.brand_code, b.brand_name, count(*)
from items i
left join brands b
on b.brand_code = i.brand_code
group by i.brand_code, b.brand_name
order by count(*) desc
limit 5;

-- Question 2 (How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?)
with month_year as (
    select distinct
	extract(month from date_scanned) as month,
	extract(year from date_scanned) as year
    from src_receipts 
),
months_ago as (
    select month, year, 
	row_number() over(order by year desc, month desc) as months_ago
    from month_year
),
recent as (
    select month, year 
    from months_ago where months_ago = 2
),
receipts as (
    select receipt_id
    from src_receipts r
    inner join recent rec
    on extract(year from r.date_scanned) = rec.year
    and extract(month from r.date_scanned) = rec.month
),
items as (
    select i.brand_code
    from src_items i
    inner join receipts r
    on i.receipt_id = r.receipt_id
    where i.src_file = 'receipts.json'
),
brands as (
    select brand_name, brand_code
    from src_brands
)
select i.brand_code, b.brand_name, count(*)
from items i
left join brands b
on b.brand_code = i.brand_code
group by i.brand_code, b.brand_name
order by count(*) desc
limit 5;

-- Question 5 (Which brand has the most spend among users who were created within the past 6 months?)
with six_months_ago_date as(
    select max(created_date) - interval '6 months' as six_months_ago from src_users
),
users as (
    select user_id
	from src_users
    where created_date >= (select six_months_ago from six_months_ago_date)
),
receipts as (
	select rcpts.receipt_id
	from src_receipts rcpts
    inner join users usr
    on rcpts.user_id = usr.user_id
),
items as (
	select itm.brand_code, sum(itm.final_price) as total_spent
	from src_items itm
    inner join receipts rcpts
	on itm.receipt_id = rcpts.receipt_id
	group by itm.brand_code
), 
brands as (
	select brand_code, brand_name from src_brands
)
select items.brand_code, brands.brand_name, sum(total_spent) as brand_spend
from items
left join brands
on items.brand_code = brands.brand_code
group by items.brand_code, brands.brand_name
order by brand_spend desc;

-- Question 6 (Which brand has the most transactions among users who were created within the past 6 months?)
with six_months_ago_date as(
    select max(created_date) - interval '6 months' as six_months_ago from src_users
),
users as (
    select user_id
	from src_users
    where created_date >= (select six_months_ago from six_months_ago_date)
),
receipts as (
	select rcpts.receipt_id
	from src_receipts rcpts
    inner join users usr
    on rcpts.user_id = usr.user_id
),
items as (
	select itm.brand_code, count(itm.receipt_id) as brand_transactions
	from src_items itm
    inner join receipts rcpts
	on itm.receipt_id = rcpts.receipt_id
	group by itm.brand_code
), 
brands as (
	select brand_code, brand_name from src_brands
)
select items.brand_code, brands.brand_name, items.brand_transactions
from items
left join brands
on items.brand_code = brands.brand_code
order by brand_transactions desc;