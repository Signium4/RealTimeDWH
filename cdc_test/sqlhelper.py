import os
import datetime
import sqlalchemy
import random as r
import pandas as pd
import cdc_test.value_generators as vg

from sqlalchemy import text
from cdc_test.get_ddls import (get_clickhouse_ddl, get_clickhouse_channels_ddl, get_clickhouse_transaction_type_ddl,
                               get_clickhouse_accounts_ddl, get_clickhouse_clients_ddl, get_clickhouse_transactions_ddl,
                               get_mysql_ddl, get_mysql_transactions_ddl, get_mysql_clients_ddl,
                               get_mysql_accounts_ddl, get_mysql_transaction_type_ddl, get_mysql_channels_ddl)
from clickhouse_driver import Client
from clickhouse_sqlalchemy import make_session

CONN_PARAMS = {
     'MYSQL_USER': 'root',
     'MYSQL_PASSWORD': 'debezium',
     'MYSQL_HOST': 'localhost',
     'MYSQL_PORT': 3306,
     'MYSQL_DATABASE': 'cdc',
     'CH_USER': 'default',
     'CH_PASSWORD': 'default_password',
     'CH_HOST': 'localhost',
     'CH_PORT': 8123
 }

class SQLHelper:
    def __init__(self, insert_count: int = 0, update_count: int = 0, delete_count: int = 0,
                 insert_count_clients: int = 0, update_count_clients: int = 0, delete_count_clients: int = 0,
                 insert_count_accounts: int = 0, delete_count_accounts: int = 0,
                 insert_count_transactions: int = 0, update_count_transactions: int = 0,
                 delete_count_transactions: int = 0,
                 count_log: bool = True):
        # Test table
        self.insert_count = insert_count
        self.update_count = update_count
        self.delete_count = delete_count
        # Clients table
        self.insert_count_clients = insert_count_clients
        self.update_count_clients = update_count_clients
        self.delete_count_clients = delete_count_clients
        # Accounts table
        self.insert_count_accounts = insert_count_accounts
        self.delete_count_accounts = delete_count_accounts
        # Transactions table
        self.insert_count_transactions = insert_count_transactions
        self.update_count_transactions = update_count_transactions
        self.delete_count_transactions = delete_count_transactions
        # Main info
        self.count_log = count_log
        self.conn_params = CONN_PARAMS

        if self.count_log:
            # Test table
            print(f"Number of INSERT operations in the table Test: {self.insert_count}")
            print(f"Number of UPDATE operations in the table Test: {self.update_count}")
            print(f"Number of DELETE operations in the table Test: {self.delete_count}")
            # Clients table
            print(f"Number of INSERT operations in the table Clients: {self.insert_count_clients}")
            print(f"Number of UPDATE operations in the table Clients: {self.update_count_clients}")
            print(f"Number of DELETE operations in the table Clients: {self.delete_count_clients}")
            # Accounts table
            print(f"Number of INSERT operations in the table Accounts: {self.insert_count_accounts}")
            print(f"Number of DELETE operations in the table Accounts: {self.delete_count_accounts}")
            # Transactions table
            print(f"Number of INSERT operations in the table Transactions: {self.insert_count_transactions}")
            print(f"Number of UPDATE operations in the table Transactions: {self.update_count_transactions}")
            print(f"Number of DELETE operations in the table Transactions: {self.delete_count_transactions}")

    def run_transactions(self):
        # Test table
        if self.insert_count > 0:
            self.insert_row()
        if self.update_count > 0:
            self.update_row()
        if self.delete_count > 0:
            self.delete_row()
        # Clients table
        if self.insert_count_clients > 0:
            self.insert_row_clients()
        if self.update_count_clients > 0:
            self.update_row_clients()
        if self.delete_count_clients > 0:
            self.delete_row_clients()
        # Accounts table
        if self.insert_count_accounts > 0:
            self.insert_row_accounts()
        if self.delete_count_accounts > 0:
            self.delete_row_accounts()
        # Transactions table
        if self.insert_count_transactions > 0:
            self.insert_row_transactions()
        if self.update_count_transactions > 0:
            self.update_row_transactions()
        if self.delete_count_transactions > 0:
            self.delete_row_transactions()
        print("The transactions were completed successfully.")

    def create_mysql_table(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_ddl()))
        print("Test table has been created in MySQL.")

    def create_mysql_table_clients(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_clients_ddl()))
        print("Clients table has been created in MySQL.")

    def create_mysql_table_accounts(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_accounts_ddl()))
        print("Accounts table has been created in MySQL.")

    def create_mysql_table_transaction_type(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_transaction_type_ddl()))
            conn.commit()

        print("Transaction type table has been created in MySQL.")

    def create_mysql_table_channels(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_channels_ddl()))
            conn.commit()

        print("Channels table has been created in MySQL.")

    def create_mysql_table_transactions(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_transactions_ddl()))

        print("Transactions table has been created in MySQL.")

    def mysql_insert_channels_tr_type(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"
        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")
        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(
                """
                    INSERT INTO cdc.channels
                    (channel_name, channel_type, device_type, security_level)
                    VALUES
                    ('Головное отделение', 'branch', NULL, 'high'),
                    ('Банкомат №42', 'atm', 'NCR', 'medium'),
                    ('Мобильное приложение', 'mobile', 'iOS', 'high'),
                    ('Интернет-банк', 'web', 'Chrome', 'high'),
                    ('Платежный терминал', 'pos', 'Ingenico', 'medium'),
                    ('Партнерский API', 'api', NULL, 'low'),
                    ('Филиал Центральный', 'branch', NULL, 'medium'),
                    ('Банкомат №128', 'atm', 'Diebold', 'medium'),
                    ('Мобильный банк', 'mobile', 'Android', 'high'),
                    ('Касса операционная', 'branch', NULL, 'high');
                """
            ))
            conn.commit()
            conn.execute(text(
                """
                    INSERT INTO cdc.transaction_type
                    (type_code, type_name, direction, category, description)
                    VALUES
                    ('TRANSF', 'Перевод между счетами', 'debit', 'transfer', 'Внутрибанковский перевод'),
                    ('CASHIN', 'Внесение наличных', 'credit', 'cash', 'Через кассу/банкомат'),
                    ('PAYMENT', 'Оплата услуг', 'debit', 'payment', 'Коммунальные платежи, налоги'),
                    ('SALARY', 'Зарплата', 'credit', 'income', 'Зачисление зарплаты'),
                    ('CASHOUT', 'Снятие наличных', 'debit', 'cash', 'В банкомате или кассе'),
                    ('REFUND', 'Возврат платежа', 'credit', 'refund', 'Отмена операции'),
                    ('FEE', 'Комиссия', 'debit', 'fee', 'Банковская комиссия'),
                    ('LOAN', 'Кредит', 'credit', 'loan', 'Зачисление кредитных средств'),
                    ('DEPOSIT', 'Пополнение депозита', 'debit', 'savings', 'Вклад в банке'),
                    ('INTEREST', 'Проценты', 'credit', 'income', 'Начисленные проценты');
                """
            ))
            conn.commit()
            print("Added data to the Channels and Transaction type tables")

    def create_clickhouse_table(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_ddl())

        print("Test table has been created in ClickHouse.")

    def create_clickhouse_table_clients(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_clients_ddl())

        print("Clients table has been created in ClickHouse.")

    def create_clickhouse_table_accounts(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_accounts_ddl())

        print("Accounts table has been created in ClickHouse.")

    def create_clickhouse_table_transaction_type(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_transaction_type_ddl())

        print("Transaction type table has been created in ClickHouse.")

    def create_clickhouse_table_channels(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_channels_ddl())

        print("Channels table has been created in ClickHouse.")

    def create_clickhouse_table_transactions(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_transactions_ddl())

        print("Transactions table has been created in ClickHouse.")

    def create_tables(self):
        # MySQL Tables
        self.create_mysql_table()
        self.create_mysql_table_clients()
        self.create_mysql_table_accounts()
        self.create_mysql_table_transaction_type()
        self.create_mysql_table_channels()
        self.create_mysql_table_transactions()
        # ClickHouse tables
        self.create_clickhouse_table()
        self.create_clickhouse_table_clients()
        self.create_clickhouse_table_accounts()
        self.create_clickhouse_table_transaction_type()
        self.create_clickhouse_table_channels()
        self.create_clickhouse_table_transactions()
        # Filing in data for Channels and Transaction type
        self.mysql_insert_channels_tr_type()

    def get_mysql_connection(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"
        db = "cdc"
        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}/{db}")
        connection = engine.connect()

        return connection

    def execute_query(self, query: str):
        with self.get_mysql_connection() as conn:
            res = conn.execute(text(query))
            conn.commit()
            return res

    def insert_row(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (INSERT): {start}")
        for i in range(self.insert_count):
            query = f"""
                INSERT INTO test_table(
                    int_val, 
                    str_val, 
                    double_val
                )
                VALUES (
                    {vg.generate_int_value()},
                    '{vg.generate_str_value()}',
                    {vg.generate_double_value()}
                )
            """
            #print(query)
            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (INSERT): {end}")
        print(f"Transactions were completed in {(end - start).seconds} seconds.")

    def insert_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (INSERT) into the Clients table: {start}")
        for i in range(self.insert_count_clients):
            query = f"""
                INSERT INTO clients(
                    family_name,
                    name,
                    middle_name,
                    email,
                    phone,
                    tax_id,
                    birth_date,
                    gender,
                    country,
                    city
                )
                VALUES (
                    '{vg.generate_family_name()}',
                    '{vg.generate_name()}',
                    '{vg.generate_middle_name()}',
                    '{vg.generate_email()}',
                    '{vg.generate_phone()}',
                    '{vg.generate_tax_id()}',
                    '{vg.generate_date()}',
                    '{vg.generate_gender()}',
                    '{vg.generate_country()}',
                    '{vg.generate_city()}'
                )
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (INSERT) in the Clients table: {end}")
        print(f"Transactions to the Clients table were completed in {(end - start).seconds} seconds.")

    def insert_row_accounts(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (INSERT) into the Accounts table: {start}")
        for i in range(self.insert_count_accounts):
            query = f"""
                INSERT INTO accounts(
                    client_id,
                    account_number,
                    account_type,
                    open_date
                )
                VALUES (
                    {self.get_random_id_clients()},
                    '{vg.generate_account_number()}',
                    '{vg.generate_account_type()}',
                    '{vg.generate_date(0, 10)}'
                )
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (INSERT) in the Accounts table: {end}")
        print(f"Transactions to the Accounts table were completed in {(end - start).seconds} seconds.")

    def insert_row_transactions(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (INSERT) into the Transactions table: {start}")
        for i in range(self.insert_count_transactions):
            query = f"""
                INSERT INTO transactions(
                    account_id,
                    type_id,
                    channel_id,
                    amount,
                    currency,
                    transaction_date,
                    description,
                    status
                )
                VALUES (
                    {self.get_random_id_accounts()},
                    {self.get_random_id_transaction_type()},
                    {self.get_random_id_channels()},
                    {vg.generate_double_value()},
                    'RUB',
                    '{vg.generate_transaction_date()}',
                    '{vg.generate_str_value()}',
                    '{vg.generate_transaction_status()}'
                )
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (INSERT) in the Transactions table: {end}")
        print(f"Transactions to the Transactions table were completed in {(end - start).seconds} seconds.")

    def delete_row(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (DELETE): {start}")

        idxs = []

        for i in range(self.delete_count):
            idx = self.get_random_id()
            idxs.append(idx)
            query = f"""
                DELETE FROM test_table 
                WHERE id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (DELETE): {end}")
        print(f"Transactions were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes were deleted: {idxs}")

    def delete_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (DELETE) in the Clients table: {start}")

        idxs = []

        for i in range(self.delete_count_clients):
            idx = self.get_random_id_clients()
            idxs.append(idx)
            query = f"""
                DELETE FROM clients 
                WHERE id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (DELETE) to the Clients table: {end}")
        print(f"Transactions to the Clients table were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes were deleted in the Clients table: {idxs}")

    def delete_row_accounts(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (DELETE) in the Accounts table: {start}")

        idxs = []

        for i in range(self.delete_count_accounts):
            idx = self.get_random_id_accounts()
            idxs.append(idx)
            query = f"""
                DELETE FROM accounts 
                WHERE account_id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (DELETE) in the Accounts table: {end}")
        print(f"Transactions in the Accounts table were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes were deleted in the Accounts table: {idxs}")

    def delete_row_transactions(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (DELETE) in the Transactions table: {start}")
        idxs = []
        for i in range(self.delete_count_transactions):
            idx = self.get_random_id_transactions()
            idxs.append(idx)
            query = f"""
                DELETE FROM transactions 
                WHERE transaction_id = {idx}
            """
            self.execute_query(query)
        end = datetime.datetime.now()
        print(f"End of transactions (DELETE) in the Transactions table: {end}")
        print(f"The transactions in the Transactions table were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes were deleted in the Transactions table: {idxs}")

    def update_row(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (UPDATE): {start}")

        idxs = []

        for i in range(self.update_count):
            idx = self.get_random_id()
            idxs.append(idx)

            query = f"""
                UPDATE test_table
                SET int_val = {vg.generate_int_value()},
                    str_val = '{vg.generate_str_value()}',
                    double_val = {vg.generate_double_value()}
                WHERE id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of Transactions (UPDATE): {end}")
        print(f"Transactions were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes have been changed: {idxs}")

    def update_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (UPDATE) in the Clients table: {start}")

        idxs = []

        for i in range(self.update_count_clients):
            idx = self.get_random_id_clients()
            idxs.append(idx)

            query = f"""
                UPDATE clients
                SET email = '{vg.generate_email()}',
                    phone = '{vg.generate_phone()}',
                    status = '{vg.generate_status()}',
                    country = '{vg.generate_country()}',
                    city ='{vg.generate_city()}' 
                WHERE id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (UPDATE) in the Clients table: {end}")
        print(f"Transactions in the Clients table were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes have been changed: {idxs}")

    def update_row_transactions(self) -> None:
        start = datetime.datetime.now()
        print(f"Start of transactions (UPDATE) in the Transactions table: {start}")

        idxs = []

        for i in range(self.update_count_transactions):
            idx = self.get_random_id_transactions()
            idxs.append(idx)

            query = f"""
                UPDATE transactions
                SET type_id = {self.get_random_id_transaction_type()},
                    amount = {vg.generate_double_value()},
                    description = '{vg.generate_str_value()}',
                    status = '{vg.generate_transaction_status()}'
                WHERE transaction_id = {idx}
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"End of transactions (UPDATE) in the Transactions table: {end}")
        print(f"Transactions in the Transactions table were completed in {(end - start).seconds} seconds.")
        print(f"Records with the following indexes have been changed: {idxs}")

    def get_random_id(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT id
                FROM test_table
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def get_random_id_clients(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT id
                FROM clients
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def get_random_id_accounts(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT account_id
                FROM accounts
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def get_random_id_transaction_type(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT account_id
                FROM accounts
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def get_random_id_channels(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT channel_id
                FROM channels
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def get_random_id_transactions(self) -> int:
        min_max = (
            self.execute_query(f"""
                SELECT transaction_id
                FROM transactions
            """)
            .fetchall()
        )

        return int(*r.choice(min_max))

    def read_clickhouse_table(self, query: str) -> pd.DataFrame:
        ch_session = self.get_clickhouse_session()

        with ch_session.connection() as ch_connection:
            dataframe = pd.read_sql(
                sql=query,
                con=ch_connection
            )

        return dataframe

    def get_clickhouse_session(self):
        user_creds = f"{self.conn_params['CH_USER']}:{self.conn_params['CH_PASSWORD']}"
        db_creds = f"{self.conn_params['CH_HOST']}:{self.conn_params['CH_PORT']}/{self.conn_params['CH_DATABASE']}"
        extras = "connect_timeout=300&send_receive_timeout=300&sync_request_timeout=300"

        engine = sqlalchemy.create_engine(f"clickhouse+native://{user_creds}@{db_creds}?{extras}")
        session = make_session(engine=engine)

        return session
