CREATE VIEW transactions_cl
AS
SELECT
	transaction_id,
	account_id,
	type_id,
	channel_id,
	amount,
	currency,
	transaction_date,
	description,
	status,
	now() as updated_at
FROM
(
    SELECT
       	transaction_id,
		account_id,
		type_id,
		channel_id,
		amount,
		currency,
		transaction_date,
		description,
		status,
		__deleted,
        row_number() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC, delivered_at DESC) AS rn
    FROM cdc.transactions_log
)
WHERE 1=1
	and rn = 1
	and not __deleted;


CREATE VIEW clients_cl
AS
SELECT
    id,
    family_name,
    name,
    middle_name,
    email,
    phone,
    tax_id,
    birth_date,
    gender,
    status,
    country,
    city,
    now() as updated_at
FROM
(
    SELECT
        id,
        family_name,
        name,
        middle_name,
        email,
        phone,
        tax_id,
        birth_date,
        gender,
        status,
        country,
        city,
        __deleted,
        row_number() OVER (PARTITION BY id ORDER BY updated_at DESC, delivered_at DESC) AS rn
    FROM cdc.clients_log
)
WHERE 1=1
	and rn = 1
	and not __deleted;


CREATE VIEW accounts_cl
AS
SELECT
    account_id,
    client_id,
    account_number,
    account_type,
    open_date,
    now() as updated_at
FROM
(
    SELECT
        account_id,
        client_id,
        account_number,
        account_type,
        open_date,
        __deleted,
        row_number() OVER (PARTITION BY account_id ORDER BY updated_at DESC, delivered_at DESC) AS rn
    FROM cdc.accounts_log
)
WHERE 1=1
	and rn = 1
	and not __deleted;


CREATE VIEW transaction_type_cl
AS
SELECT
    type_id,
    type_code,
    type_name,
    direction,
    category,
    description,
    now() as updated_at
FROM
(
    SELECT
        type_id,
        type_code,
        type_name,
        direction,
        category,
        description,
        __deleted,
        row_number() OVER (PARTITION BY type_id ORDER BY updated_at DESC, delivered_at DESC) AS rn
    FROM cdc.transaction_type_log
)
WHERE 1=1
	and rn = 1
	and not __deleted;


CREATE VIEW channels_cl
AS
SELECT
    channel_id,
    channel_name,
    channel_type,
    device_type,
    security_level,
    now() as updated_at
FROM
(
    SELECT
        channel_id,
        channel_name,
        channel_type,
        device_type,
        security_level,
        __deleted,
        row_number() OVER (PARTITION BY channel_id ORDER BY updated_at DESC, delivered_at DESC) AS rn
    FROM cdc.channels_log
)
WHERE 1=1
	and rn = 1
	and not __deleted;
