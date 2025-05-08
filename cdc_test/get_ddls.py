def get_clickhouse_ddl() -> str:
	return """
        CREATE TABLE IF NOT EXISTS cdc.`test_table_log` (
            id Int32,
            int_val Int32,
            str_val Varchar(20),
            double_val Decimal(10, 2),
            inserted_at DateTime,
            updated_at Nullable(DateTime),
            delivered_at DateTime DEFAULT now(),
            __deleted BOOLEAN DEFAULT false
        ) 
        ENGINE=MergeTree
        ORDER BY id
        SETTINGS index_granularity = 8192;
    """


def get_clickhouse_clients_ddl() -> str:
	return """
        CREATE TABLE IF NOT EXISTS cdc.`clients_log` (
            id Int32,
            family_name String NOT NULL,
			name String NOT NULL,
			middle_name Nullable(String),
			email Nullable(String),
			phone Nullable(String),
			tax_id Nullable(String),
			birth_date Nullable(String),
			gender Nullable(String),
			status Nullable(String),
			country Nullable(String),
			city Nullable(String),
            inserted_at DateTime,
            updated_at Nullable(DateTime),
            delivered_at DateTime DEFAULT now(),
            __deleted BOOLEAN DEFAULT false
        ) 
        ENGINE=MergeTree
        ORDER BY id
        SETTINGS index_granularity = 8192;
    """

def get_clickhouse_accounts_ddl() -> str:
	return """
        CREATE TABLE IF NOT EXISTS cdc.`accounts_log`
		(
			account_id UInt32,
			client_id Nullable(UInt32),
			account_number Nullable(String),
			account_type Nullable(String),
			open_date Date NOT NULL,
            inserted_at DateTime,
            updated_at Nullable(DateTime),
            delivered_at DateTime DEFAULT now(),
            __deleted BOOLEAN DEFAULT false
        ) 
        ENGINE=MergeTree
        ORDER BY account_id
        SETTINGS index_granularity = 8192;
    """

def get_clickhouse_transaction_type_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS cdc.`transaction_type_log`
		(
			type_id UInt32,
			type_code String,
			type_name String,
			direction String,
			category String,
			description Nullable(String),
			inserted_at DateTime,
            updated_at Nullable(DateTime),
            delivered_at DateTime DEFAULT now(),
            __deleted BOOLEAN DEFAULT false
        )
        ENGINE=MergeTree
        ORDER BY type_id
        SETTINGS index_granularity = 8192;
    """

def get_clickhouse_channels_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS cdc.`channels_log`
		(			
		    channel_id UInt32,
			channel_name String NOT NULL,
			channel_type String,
			device_type Nullable(String),
			security_level String,
			inserted_at DateTime,
            updated_at Nullable(DateTime),
            delivered_at DateTime DEFAULT now(),
            __deleted BOOLEAN DEFAULT false
        )
        ENGINE=MergeTree
        ORDER BY channel_id
        SETTINGS index_granularity = 8192;
    """

def get_clickhouse_transactions_ddl() -> str:
	return """

    """

def get_mysql_ddl() -> str:
	return """
	    CREATE TABLE IF NOT EXISTS test_table(
	        id int NOT NULL AUTO_INCREMENT,
	        int_val int NOT NULL,
	        str_val VARCHAR(20),
	        double_val DECIMAL(10, 2),
	        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	        updated_at TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
	        PRIMARY KEY (`id`)
	    )
	    ENGINE=InnoDB 
	    DEFAULT CHARSET=utf8mb4;
	"""

def get_mysql_transactions_ddl() -> str:
	return """
	    
	"""


def get_mysql_transaction_type_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS transaction_type (
			type_id INT NOT NULL AUTO_INCREMENT,
			type_code VARCHAR(10) NOT NULL,
			type_name VARCHAR(50) NOT NULL,
			direction VARCHAR(10) NOT NULL,
			category VARCHAR(30) NOT NULL,
			description VARCHAR(255),
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (type_id),
			INDEX idx_category (category),
			INDEX idx_direction (direction)
		) ENGINE=InnoDB CHARSET=utf8mb4;
	"""

def get_mysql_accounts_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS accounts (
			account_id INT NOT NULL AUTO_INCREMENT,
			client_id INT NOT NULL,
			account_number VARCHAR(20) NOT NULL,
			account_type VARCHAR(20) NOT NULL,
			open_date DATE NOT NULL,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (account_id),
			INDEX idx_client (client_id),
			INDEX idx_type (account_type)
	) ENGINE=InnoDB CHARSET=utf8mb4;
	"""

def get_mysql_channels_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS channels (
			channel_id INT NOT NULL AUTO_INCREMENT,
			channel_name VARCHAR(30) NOT NULL,
			channel_type VARCHAR(10),
			device_type VARCHAR(20),
			security_level VARCHAR(10),
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (channel_id),
			INDEX idx_channel_type (channel_type),
			INDEX idx_security_level (security_level)
		) ENGINE=InnoDB CHARSET=utf8mb4;
	"""

def get_mysql_clients_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS clients (
			id INT NOT NULL AUTO_INCREMENT,
			family_name VARCHAR(50) NOT NULL,
			name VARCHAR(50) NOT NULL,
			middle_name VARCHAR(50),
			email VARCHAR(100),
			phone VARCHAR(20),
			tax_id VARCHAR(20),
			birth_date VARCHAR(10),
			gender VARCHAR(10) NOT NULL,
			status VARCHAR(20) DEFAULT 'активный',
			country VARCHAR(50),
			city VARCHAR(50),
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			INDEX idx_email (email),
			INDEX idx_phone (phone),
			INDEX idx_tax_id (tax_id),
			INDEX idx_full_name (name, family_name, middle_name),
			INDEX idx_status (status)
		)
		ENGINE=InnoDB
		DEFAULT CHARSET=utf8mb4
		COLLATE=utf8mb4_unicode_ci;
	"""