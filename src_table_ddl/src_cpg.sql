CREATE TABLE IF NOT EXISTS src_cpg (
    cpg_id TEXT,
    cpg_ref TEXT,
    PRIMARY KEY (cpg_id, cpg_ref)
);

INSERT INTO src_cpg (cpg_id, cpg_ref)
SELECT distinct trim(cpg_id), trim(cpg_ref)
FROM stg_brands;