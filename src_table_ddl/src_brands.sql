CREATE TABLE IF NOT EXISTS src_brands (
    brand_id TEXT PRIMARY KEY,
    brand_code TEXT, 
    brand_name TEXT, 
    top_brand BOOLEAN,
    category_id TEXT,
    cpg_id TEXT,
    cpg_ref TEXT,
    FOREIGN KEY (cpg_id, cpg_ref) REFERENCES src_cpg(cpg_id, cpg_ref) ON DELETE NO ACTION,
    FOREIGN KEY (category_id) REFERENCES src_brand_categories(category_id) ON DELETE NO ACTION
);

INSERT INTO src_brands (brand_id, brand_code, brand_name, top_brand, category_id, cpg_id, cpg_ref)
SELECT distinct trim(b.brand_id), trim(b.brand_code), trim(b.name), b.top_brand, c.category_id, b.cpg_id, b.cpg_ref
FROM stg_brands b
LEFT JOIN src_brand_categories c
on trim(b.category) = trim(c.category_name);