
CREATE VIEW transactions_agg_month
AS
SELECT
    -- Client information
    cl.id AS client_id,
    cl.family_name,
    cl.name,
    cl.middle_name,
    cl.email,
    cl.phone,
    -- Account information
    ac.account_id,
    ac.account_number,
    ac.account_type,
    -- Monthly transaction aggregates
    toStartOfMonth(tc.transaction_date) AS month,
    COUNT(tc.transaction_id) AS transactions_count,
    SUM(tc.amount) AS total_amount,
    COUNT(DISTINCT tc.type_id) AS unique_transaction_types,
    COUNT(DISTINCT tc.channel_id) AS unique_channels,
    -- Transaction details arrays
    groupArray(tc.transaction_id) AS transaction_ids,
    groupArray(tc.amount) AS amounts,
    groupArray(tc.currency) AS currencies,
    groupArray(tc.status) AS statuses,
    -- Transaction type information
    tt.type_code AS transaction_type_code,
    tt.type_name AS transaction_type_name,
    tt.direction AS transaction_direction,
    tt.category AS transaction_category,
    -- Channel information
    ch.channel_name,
    ch.channel_type,
    ch.security_level,
    -- Timestamp
    now() AS updated_at
FROM transactions_cl tc
INNER JOIN accounts_cl ac ON tc.account_id = ac.account_id
INNER JOIN clients_cl cl ON ac.client_id = cl.id
INNER JOIN transaction_type_cl tt ON tc.type_id = tt.type_id
INNER JOIN channels_cl ch ON tc.channel_id = ch.channel_id
GROUP BY
    -- Client fields
    cl.id,
    cl.family_name,
    cl.name,
    cl.middle_name,
    cl.email,
    cl.phone,
    -- Account fields
    ac.account_id,
    ac.account_number,
    ac.account_type,
    -- Month grouping
    toStartOfMonth(tc.transaction_date),
    -- Transaction type fields
    tt.type_code,
    tt.type_name,
    tt.direction,
    tt.category,
    -- Channel fields
    ch.channel_name,
    ch.channel_type,
    ch.security_level;


CREATE VIEW clients_agg_month
AS
SELECT
    -- Client information
    cl.id AS client_id,
    cl.family_name,
    cl.name,
    cl.middle_name,
    cl.email,
    cl.phone,
    cl.country,
    cl.city,
    -- Time dimension (month)
    toStartOfMonth(tc.transaction_date) AS month,
    -- Transaction aggregates
    COUNT(tc.transaction_id) AS transaction_count,
    SUM(tc.amount) AS total_transaction_amount,
    COUNT(DISTINCT tc.type_id) AS distinct_transaction_types,
    COUNT(DISTINCT tc.channel_id) AS distinct_channels,
    COUNT(DISTINCT ac.account_id) AS distinct_accounts,
    -- Timestamp
    now() AS updated_at
FROM clients_cl cl
INNER JOIN accounts_cl ac ON cl.id = ac.client_id
INNER JOIN transactions_cl tc ON ac.account_id = tc.account_id
GROUP BY
    cl.id,
    cl.family_name,
    cl.name,
    cl.middle_name,
    cl.email,
    cl.phone,
    cl.country,
    cl.city,
    toStartOfMonth(tc.transaction_date);