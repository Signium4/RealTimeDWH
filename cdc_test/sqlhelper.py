import os
import datetime
import sqlalchemy
import random as r
import pandas as pd
import cdc_test.value_generators as vg

from sqlalchemy import text
from cdc_test.get_ddls import (get_clickhouse_ddl, get_clickhouse_clients_ddl,
                               get_mysql_ddl, get_mysql_transactions_ddl, get_mysql_clients_ddl)
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
                 count_log: bool = True):
        self.insert_count = insert_count
        self.update_count = update_count
        self.delete_count = delete_count
        self.insert_count_clients = insert_count_clients
        self.update_count_clients = update_count_clients
        self.delete_count_clients = delete_count_clients
        self.count_log = count_log
        self.conn_params = CONN_PARAMS

        if self.count_log:
            print(f"Кол-во операций INSERT в таблице Test: {self.insert_count}")
            print(f"Кол-во операций UPDATE в таблице Test: {self.update_count}")
            print(f"Кол-во операций DELETE в таблице Test: {self.delete_count}")
            print(f"Кол-во операций INSERT в таблице Clients: {self.insert_count_clients}")
            print(f"Кол-во операций UPDATE в таблице Clients: {self.update_count_clients}")
            print(f"Кол-во операций DELETE в таблице Clients: {self.delete_count_clients}")

    def run_transactions(self):
        if self.insert_count > 0:
            self.insert_row()
        if self.update_count > 0:
            self.update_row()
        if self.delete_count > 0:
            self.delete_row()
        if self.insert_count_clients > 0:
            self.insert_row_clients()
        if self.update_count_clients > 0:
            self.update_row_clients()
        if self.delete_count_clients > 0:
            self.delete_row_clients()
        print("Транзакции были проведены успешно.")

    def create_mysql_table(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_ddl()))
        print("Создана тестовая таблица в MySQL.")

    def create_mysql_table_transactions(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_transactions_ddl()))
        print("Создана таблица Transactions в MySQL.")

    def create_mysql_table_clients(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"

        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}")

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS cdc;"))
            conn.execute(text("USE cdc;"))
            conn.execute(text(get_mysql_clients_ddl()))
        print("Создана таблица Clients в MySQL.")

    def create_clickhouse_table(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_ddl())

        print("Создана тестовая таблица в ClickHouse")

    def create_clickhouse_table_cients(self):
        client = Client(
            host=self.conn_params['CH_HOST'],
            user=self.conn_params['CH_USER'],
            password=self.conn_params['CH_PASSWORD']
        )

        client.execute("CREATE DATABASE IF NOT EXISTS cdc;")
        client.execute("USE cdc;")
        client.execute(get_clickhouse_clients_ddl())

        print("Создана таблица Clients в ClickHouse")

    def create_tables(self):
        self.create_mysql_table()
        self.create_mysql_table_clients()

        self.create_clickhouse_table()
        self.create_clickhouse_table_cients()

    def get_mysql_connection(self):
        user_creds = f"{self.conn_params['MYSQL_USER']}:{self.conn_params['MYSQL_PASSWORD']}"
        db_creds = f"{self.conn_params['MYSQL_HOST']}:{self.conn_params['MYSQL_PORT']}"
        db = "cdc"
        #extras = "charset=utf8mb4"
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
        print(f"Начало транзакций (INSERT): {start}")
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
        print(f"Конец транзакций (INSERT): {end}")
        print(f"Транзакции были выполнены за {(end - start).seconds} секунд.")

    def insert_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Начало транзакций (INSERT) в таблицу Clients: {start}")
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
                    '{vg.generate_birth_date()}',
                    '{vg.generate_gender()}',
                    '{vg.generate_country()}',
                    '{vg.generate_city()}'
                )
            """

            self.execute_query(query)

        end = datetime.datetime.now()
        print(f"Конец транзакций (INSERT) в таблицу Clients: {end}")
        print(f"Транзакции в таблицу Clients были выполнены за {(end - start).seconds} секунд.")

    def delete_row(self) -> None:
        start = datetime.datetime.now()
        print(f"Начало транзакций (DELETE): {start}")

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
        print(f"Конец транзакций (DELETE): {end}")
        print(f"Транзакции были выполнены за {(end - start).seconds} секунд.")
        print(f"Удалению подверглись записи со следующими индексами: {idxs}")

    def delete_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Начало транзакций (DELETE): {start}")

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
        print(f"Конец транзакций (DELETE) в таблицу Clients: {end}")
        print(f"Транзакции в таблицу Clients были выполнены за {(end - start).seconds} секунд.")
        print(f"Удалению в таблице Clients подверглись записи со следующими индексами: {idxs}")

    def update_row(self) -> None:
        start = datetime.datetime.now()
        print(f"Начало транзакций (UPDATE): {start}")

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
        print(f"Конец транзакций (UPDATE): {end}")
        print(f"Транзакции были выполнены за {(end - start).seconds} секунд.")
        print(f"Изменению подверглись записи со следующими индексами: {idxs}")

    def update_row_clients(self) -> None:
        start = datetime.datetime.now()
        print(f"Начало транзакций (UPDATE): {start}")

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
        print(f"Конец транзакций (UPDATE): {end}")
        print(f"Транзакции были выполнены за {(end - start).seconds} секунд.")
        print(f"Изменению подверглись записи со следующими индексами: {idxs}")

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

    def read_mysql_table(self, query) -> pd.DataFrame:

        with self.get_mysql_connection() as mysql_conn:
            dataframe = pd.read_sql(
                sql=query,
                con=mysql_conn
            )

        return dataframe