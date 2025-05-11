import argparse
from cdc_test.sqlhelper import SQLHelper

argParser = argparse.ArgumentParser()
# Test table
argParser.add_argument("--insert", type=int, default=0, help="Number of INSERT operations")
argParser.add_argument("--update", type=int, default=0, help="Number of UPDATE operations")
argParser.add_argument("--delete", type=int, default=0, help="Number of DELETE operations")
# Clients table
argParser.add_argument("--insert_clients", type=int, default=0,
                       help="Number of INSERT operations in the table Clients")
argParser.add_argument("--update_clients", type=int, default=0,
                       help="Number of UPDATE operations in the table Clients")
argParser.add_argument("--delete_clients", type=int, default=0,
                       help="Number of DELETE operations in the table Clients")
# Accounts table
argParser.add_argument("--insert_accounts", type=int, default=0,
                       help="Number of INSERT operations in the table Accounts")
argParser.add_argument("--delete_accounts", type=int, default=0,
                       help="Number of DELETE operations in the table Accounts")
# Trasactions table
argParser.add_argument("--insert_transactions", type=int, default=0,
                       help="Number of INSERT operations in the table Trasactions")
argParser.add_argument("--update_transactions", type=int, default=0,
                       help="Number of UPDATE operations in the table Trasactions")
argParser.add_argument("--delete_transactions", type=int, default=0,
                       help="Number of DELETE operations in the table Trasactions")
args = argParser.parse_args()

helper = SQLHelper(args.insert, args.update, args.delete,
                   args.insert_clients, args.update_clients, args.delete_clients,
                   args.insert_accounts, args.delete_accounts,
                   args.insert_transactions, args.update_transactions, args.delete_transactions)
helper.run_transactions()
