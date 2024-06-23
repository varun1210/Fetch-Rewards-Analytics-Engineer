CREATE TABLE IF NOT EXISTS stg_receipts (
    receipt_id TEXT,
    bonus_points_earned NUMERIC(15, 2),
    bonus_points_earned_reason TEXT,
    create_date TIMESTAMP WITH TIME ZONE,
    date_scanned TIMESTAMP WITH TIME ZONE,
    finished_date TIMESTAMP WITH TIME ZONE,
    modify_date TIMESTAMP WITH TIME ZONE,
    points_awarded_date TIMESTAMP WITH TIME ZONE,
    points_earned  NUMERIC(15, 2),
    purchase_date TIMESTAMP WITH TIME ZONE,
    purchased_item_count INTEGER,
    rewards_receipt_status TEXT,
    total_spent  NUMERIC(10, 2),
    user_id TEXT,
    contains_items BOOLEAN
);















