import argparse
from cdc_test.sqlhelper import SQLHelper

argParser = argparse.ArgumentParser()
# Test table
argParser.add_argument("--insert", type=int, default=0, help="Кол-во операций INSERT")
argParser.add_argument("--update", type=int, default=0, help="Кол-во операций UPDATE")
argParser.add_argument("--delete", type=int, default=0, help="Кол-во операций DELETE")
# Clients table
argParser.add_argument("--insert_clients", type=int, default=0,
                       help="Кол-во операций INSERT в таблице Clients")
argParser.add_argument("--update_clients", type=int, default=0,
                       help="Кол-во операций UPDATE в таблице Clients")
argParser.add_argument("--delete_clients", type=int, default=0,
                       help="Кол-во операций DELETE в таблице Clients")
# Accounts table
argParser.add_argument("--insert_accounts", type=int, default=0,
                       help="Кол-во операций INSERT в таблице Accounts")
argParser.add_argument("--delete_accounts", type=int, default=0,
                       help="Кол-во операций DELETE в таблице Accounts")
# Trasactions table
argParser.add_argument("--insert_transactions", type=int, default=0,
                       help="Кол-во операций INSERT в таблице Trasactions")
argParser.add_argument("--update_transactions", type=int, default=0,
                       help="Кол-во операций UPDATE в таблице Trasactions")
argParser.add_argument("--delete_transactions", type=int, default=0,
                       help="Кол-во операций DELETE в таблице Trasactions")
args = argParser.parse_args()

helper = SQLHelper(args.insert, args.update, args.delete,
                   args.insert_clients, args.update_clients, args.delete_clients,
                   args.insert_accounts, args.delete_accounts,
                   args.insert_transactions, args.update_transactions, args.delete_transactions)
helper.run_transactions()
