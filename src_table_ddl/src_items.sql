CREATE TABLE IF NOT EXISTS src_items(
	receipt_id TEXT,
	barcode TEXT,
	item_description TEXT,
	brand_code TEXT,
	item_number TEXT,
	competitive_product BOOLEAN,
	rewards_group TEXT,
	competitor_rewards_group TEXT,
	deleted BOOLEAN,
	quantity_purchased INTEGER,
	item_price NUMERIC(9, 2),
	target_price NUMERIC(9, 2),
	discounted_item_price NUMERIC(9, 2),
	price_after_coupon NUMERIC(9, 2),
	final_price NUMERIC(9, 2),
	partner_item_id TEXT,
	rewards_product_partner_id TEXT,
	points_payer_id TEXT,
	points_earned NUMERIC(15, 2),
	needs_fetch_review BOOLEAN,
	needs_fetch_review_reason TEXT,
	points_not_awarded_reason TEXT,
	prevent_target_gap_points BOOLEAN,
	user_flagged_barcode TEXT,
	user_flagged_new_item BOOLEAN,
	user_flagged_price NUMERIC(9, 2),
	user_flagged_quantity INTEGER,
	user_flagged_description TEXT,
	original_meta_brite_barcode TEXT,
	original_meta_brite_description TEXT,
	original_receipt_item_text TEXT,
	original_meta_brite_quantity_purchased INTEGER,
	original_final_price NUMERIC(9, 2),
	original_meta_brite_item_price NUMERIC(9, 2),
	metabrite_campaign_id TEXT,
    src_file TEXT,
	FOREIGN KEY (receipt_id) REFERENCES src_receipts (receipt_id) ON DELETE NO ACTION
);

INSERT INTO src_items (
    receipt_id,
    barcode,
    item_description,
    brand_code,
    item_number,
    competitive_product,
    rewards_group,
    competitor_rewards_group,
    deleted,
    quantity_purchased,
    item_price,
    target_price,
    discounted_item_price,
    price_after_coupon,
    final_price,
    partner_item_id,
    rewards_product_partner_id,
    points_payer_id,
    points_earned,
    needs_fetch_review,
    needs_fetch_review_reason,
    points_not_awarded_reason,
    prevent_target_gap_points,
    user_flagged_barcode,
    user_flagged_new_item,
    user_flagged_price,
    user_flagged_quantity,
    user_flagged_description,
    original_meta_brite_barcode,
    original_meta_brite_description,
    original_receipt_item_text,
    original_meta_brite_quantity_purchased,
    original_final_price,
    original_meta_brite_item_price,
    metabrite_campaign_id, 
    src_file
)
SELECT distinct 
receipt_id,
barcode,
description,
brand_code,
item_number,
competitive_product,
rewards_group,
competitor_rewards_group,
deleted,
quantity_purchased,
item_price,
target_price,
discounted_item_price,
price_after_coupon,
final_price,
partner_item_id,
rewards_product_partner_id,
points_payer_id,
points_earned,
needs_fetch_review,
needs_fetch_review_reason,
points_not_awarded_reason,
prevent_target_gap_points,
user_flagged_barcode,
user_flagged_new_item,
user_flagged_price,
user_flagged_quantity,
user_flagged_description,
original_meta_brite_barcode,
original_meta_brite_description,
original_receipt_item_text,
original_meta_brite_quantity_purchased,
original_final_price,
original_meta_brite_item_price,
metabrite_campaign_id,
'receipts.json'
FROM stg_receipts_items ri;

INSERT INTO src_items (
    barcode,
    brand_code,
    src_file
)
SELECT distinct trim(barcode), trim(brand_code), 'brands.json'
FROM stg_brands
WHERE barcode not in (SELECT distinct barcode from stg_receipts_items);
