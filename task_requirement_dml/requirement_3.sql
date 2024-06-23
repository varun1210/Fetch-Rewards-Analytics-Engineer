-- Query to scan for duplicates in users file:
select *, count(*) as number_of_records
from stg_users
group by
user_id,
state,
created_date,
last_login,
role,
active,
sign_up_source
having count(*) > 1

-- Query to find which users are NOT 'consumer'
select * from stg_users where trim(role) <> 'consumer';

-- Query to find duplicate barcodes in brands.json:
select barcode, count(barcode) as barcode_occurance
from stg_brands 
group by barcode
having count(barcode) > 1;

-- Query to further investigate barcodes in brands.json:
select * from stg_brands where trim(barcode) in 
('511111605058', '511111204923', '511111704140', '511111504788', '511111504139', '511111305125', '511111004790')
order by barcode;

-- Query to look at brandCode inconsistencies in brands.json:
select distinct brand_code from stg_brands;

-- Query to look at name inconsistencie in brands.json:
select distinct brand_code from stg_brands; 

-- Query to find receipts of users who do not exist in the users.json:
select distinct user_id from stg_receipts where user_id not in 
(select user_id from stg_users);

-- Query to find receipts without receipt items:
select * from stg_receipts where contains_items is false;

-- Query to view differnet barcode discrepencies for various in the receipts:
select distinct(barcode) from stg_items;

-- Query to find items in receipts that do not have a barcode:
select * from stg_receipt_items where barcode is null;

-- Query to find items with no brandCode
select * from stg_receipts_items where brand_code is null

-- Query to find items in receipts that don't have a description:
select * from stg_receipt_items where description is null or description = 'ITEM NOT FOUND';