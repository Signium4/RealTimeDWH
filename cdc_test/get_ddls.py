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
			birth_date Nullable(Date),
			gender Enum('М' = 1, 'Ж' = 2),
			status Enum('активный' = 1, 'неактивный' = 2),
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

	"""

def get_mysql_accounts_ddl() -> str:
	return """
		CREATE TABLE IF NOT EXISTS accounts (
		account_id INT NOT NULL AUTO_INCREMENT,
		client_id INT NOT NULL,
		account_number VARCHAR(20) NOT NULL,
		account_type VARCHAR(20) NOT NULL,
		open_date DATE NOT NULL,
		PRIMARY KEY (account_id),
		INDEX idx_client (client_id),
		INDEX idx_type (account_type)
	) ENGINE=InnoDB CHARSET=utf8mb4;
	"""

def get_mysql_channel_ddl() -> str:
	return """

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
			birth_date DATE,
			gender ENUM('М', 'Ж'),
			status ENUM('активный', 'неактивный') DEFAULT 'активный',
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