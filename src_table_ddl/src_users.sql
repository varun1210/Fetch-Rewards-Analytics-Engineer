CREATE TABLE IF NOT EXISTS src_users (
    user_id TEXT PRIMARY KEY,
    role TEXT,
    active BOOLEAN,
    sign_up_source TEXT,
    state TEXT,
    created_date TIMESTAMP WITH TIME ZONE,
    last_login_date TIMESTAMP WITH TIME ZONE
);

INSERT INTO src_users (user_id, role, active, sign_up_source, state, created_date, last_login_date)
SELECT distinct trim(user_id), trim(role), active, trim(sign_up_source), trim(state),
date_trunc('second', created_date),
date_trunc('second', last_login)
FROM stg_users;