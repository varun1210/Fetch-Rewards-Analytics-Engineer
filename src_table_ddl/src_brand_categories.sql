CREATE TABLE IF NOT EXISTS src_brand_categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT,
    category_code TEXT
);

INSERT INTO src_brand_categories (category_id, category_name, category_code)
SELECT 
replace(gen_random_uuid()::TEXT, '-', ''), 
category_name,
category_code
FROM (
    SELECT DISTINCT 
    trim(category) AS category_name,
    CASE
    WHEN trim(category) = 'Health & Wellness' THEN 'HEALTHY_AND_WELLNESS'
    ELSE coalesce(trim(category_code), upper(replace(replace(trim(category), ' ', '_'), '&', 'AND'))) 
    END AS category_code
    FROM stg_brands where category is not null
) brand_cat;
