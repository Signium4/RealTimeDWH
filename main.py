import argparse
from cdc_test.sqlhelper import SQLHelper

argParser = argparse.ArgumentParser()
argParser.add_argument("--insert", type=int, default=0, help="Кол-во операций INSERT")
argParser.add_argument("--update", type=int, default=0, help="Кол-во операций UPDATE")
argParser.add_argument("--delete", type=int, default=0, help="Кол-во операций DELETE")
argParser.add_argument("--insert_clients", type=int, default=0,
                       help="Кол-во операций INSERT в таблице Clients")
argParser.add_argument("--update_clients", type=int, default=0,
                       help="Кол-во операций UPDATE в таблице Clients")
argParser.add_argument("--delete_clients", type=int, default=0,
                       help="Кол-во операций DELETE в таблице Clients")
args = argParser.parse_args()

helper = SQLHelper(args.insert, args.update, args.delete, args.insert_clients, args.update_clients, args.delete_clients)
helper.run_transactions()