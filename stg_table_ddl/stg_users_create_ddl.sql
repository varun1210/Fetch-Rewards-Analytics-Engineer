CREATE TABLE IF NOT EXISTS stg_users (
    user_id TEXT,
    state TEXT,
    created_date TIMESTAMP WITH TIME ZONE,
    last_login TIMESTAMP WITH TIME ZONE,
    role TEXT,
    active BOOLEAN,
    sign_up_source TEXT
);