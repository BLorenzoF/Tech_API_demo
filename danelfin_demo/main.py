import os
import sys
import argparse # Required to introduce the data with flags
from module_method.functions import CustomerManager # Where functions.py module is imported from, Getting the get_customer, add_client and dump methods
from tinydb import TinyDB, Query #Importing TinyDB for the creation of the database and Query to use in the "get_customer" method.
from loguru import logger
#import fire 


db = TinyDB('db.json') # declaring db variable.

def main(): #Main function, with argparse you can input different flags. With more time I would've used fire to simplify this script.
    logger.add("output.log", rotation="20 MB") #Configuring logger output file

    parser = argparse.ArgumentParser(description="Insert the values that will be used by the corresponding method.")
    parser.add_argument("method", choices=["add-customer", "get-client",'dump'], help="Method to run") # Select which method will be run
    parser.add_argument("--name", type=str, help="Name", required=False)
    parser.add_argument("--email", type=str, help="Email", required=False)
    parser.add_argument("--age", type=int, help="Age", required=False)
    parser.add_argument("--country", type=str, help="Country", required=False)
    parser.add_argument("--id", type=int, help="Id to query when using get_customer", required=False)
    args = parser.parse_args()

    if args.method == "add-customer": #If method selected is add-customer, the following code will be executed
        customer_manager = CustomerManager()
        try:
            customer_info = customer_manager.add_customer(name=args.name, email=args.email, age=args.age,
                                                           country=args.country)
            print("Customer added:", customer_info)
            db.insert(customer_info)
            sys.exit('Customer added succesfully')
        except ValueError as e: #add_customer Error display.
            print("Error adding customer:", e)

    if args.method == "get-client":#If method selected is get-client, the following code will be executed
        customer_manager = CustomerManager()
        try:
            customer_id = customer_manager.get_client(id= args.id)
            print("Customer retrieved:", customer_id)
            sys.exit('Customer retrieved succesfully') ### To-do: Add option if id doesn't exist in database.
        except ValueError as e:#get-client Error display.
            print("Error retrieving customer:", e)

    if args.method == "dump":#If method selected is dump, the following code will be executed
        customer_manager = CustomerManager()
        try:
            logger.debug("This is a debug message")
            customer_id = customer_manager.dump(db)
            sys.exit('dumped succesfully')
        except ValueError as e:#dump Error display.
            print("Error dumping:", e)
    else:
        print("Invalid method specified.") #If input doesn't adjust to the method, this will show.

if __name__ == "__main__":
    main() 
