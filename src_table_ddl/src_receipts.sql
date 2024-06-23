CREATE TABLE IF NOT EXISTS src_receipts (
    receipt_id TEXT PRIMARY KEY, 
    user_id TEXT, 
    bonus_points_earned NUMERIC(10, 2),
    bonus_points_earned_reason TEXT,
    points_earned NUMERIC(10, 2),
    purchased_item_count INTEGER,
    rewards_receipt_status TEXT,
    total_spent NUMERIC(9, 2),
    contains_items BOOLEAN,
    create_date TIMESTAMP WITH TIME ZONE,
    date_scanned TIMESTAMP WITH TIME ZONE,
    finished_date TIMESTAMP WITH TIME ZONE,
    modify_date TIMESTAMP WITH TIME ZONE,
    points_awarded_date TIMESTAMP WITH TIME ZONE,
    purchase_date TIMESTAMP WITH TIME ZONE
);

INSERT INTO src_receipts(receipt_id, user_id, bonus_points_earned, bonus_points_earned_reason, points_earned, purchased_item_count, rewards_receipt_status, total_spent, contains_items, create_date, date_scanned, finished_date, modify_date, points_awarded_date, purchase_date)
SELECT distinct trim(receipt_id), trim(user_id), bonus_points_earned, trim(bonus_points_earned_reason), points_earned, purchased_item_count, trim(rewards_receipt_status), total_spent, contains_items, date_trunc('second', create_date), date_trunc('second', date_scanned), date_trunc('second', finished_date), date_trunc('second', modify_date), date_trunc('second', points_awarded_date), date_trunc('second', purchase_date)
FROM stg_receipts;