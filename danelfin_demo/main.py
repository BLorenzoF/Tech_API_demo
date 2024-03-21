import os
import sys
import argparse # Required to introduce the data with flags
from pydantic import BaseModel, EmailStr # BaseModel for pydantic to understand the format we should introduce the data, EmailStr to validate the e-mail
from tinydb import TinyDB, Query #Importing TinyDB for the creation of the database and Query to use in the "get_customer" method.
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
from loguru import logger

logger.debug("That's it, beautiful and simple logging!")

db = TinyDB('db.json') # declaring db variable.

class Customer(BaseModel): # Creation of customer class for add_customer
    name: str
    email: EmailStr
    age: int
    country: str

class CustomerManager: # Customer manager class will handle the methods.

    def __init__(self):
        self.customers = []
        self.id = self.get_id() # will update id for it's output.
        self.db = db # introducing variable for get_id method

    def add_customer(self, name: str, email: str, age: int, country: str): # add_customer method. Retrieves customer from BaseModel for e-mail validation
        customer = Customer(name=name, email=email, age=age, country=country)
        self.customers.append(customer.dict())
        customer_dict = customer.dict()
        customer_dict['id'] = self.id # adds
        return customer_dict

    def get_id(self): #method that gets the len of the database to get it's last ID, not fool-proof to removal of rows.
        last_row = db.get(doc_id=len(db))
        if last_row:
            self.id = last_row.doc_id + 1
        else:
            self.id = 1
        return self.id

    def get_customer(self, id): ##MISSING error if id doesn't exist in database.
        User = Query()
        customer = self.db.search(User.id == id)
        return(customer)

    def dump(self, db): # method for dumping a parquet file
        pd_dataset = pd.DataFrame(db.all()) # convert db.all to a pandas dataset
        pa_dataset = pa.Table.from_pandas(pd_dataset) #convert pandas dataset to pyarrow dataset
        my_schema = pa.schema([('name', pa.string()),
                               ('email', pa.string()),
                               ('age', pa.int64()),
                               ('country', pa.string()),
                               ('id', pa.int64())]) # Define the schema for the dump
        partition_scheme = ds.partitioning(pa.schema([pa.field('country', pa.string())]), flavor=None) #Define partitioning object
        parquet_file = pq.write_to_dataset(table =pa_dataset ,root_path=  os.getcwd(), partitioning = partition_scheme, schema=my_schema, basename_template= 'dump{i}.parquet', existing_data_behavior= 'overwrite_or_ignore')
        return parquet_file

def main(): #Main function, with argparse you can input different flags.
    parser = argparse.ArgumentParser(description="Inserts a customer.")
    parser.add_argument("method", choices=["add_customer", "get_customer",'dump'], help="Method to run") #
    parser.add_argument("--name", type=str, help="Name", required=False)
    parser.add_argument("--email", type=str, help="Email", required=False)
    parser.add_argument("--age", type=int, help="Age", required=False)
    parser.add_argument("--country", type=str, help="Country", required=False)
    parser.add_argument("--id", type=int, help="Id to query when using get_customer", required=False)
    args = parser.parse_args()

    if args.method == "add_customer":
        customer_manager = CustomerManager()
        try:
            customer_info = customer_manager.add_customer(name=args.name, email=args.email, age=args.age,
                                                           country=args.country)
            print("Customer added:", customer_info)
            db.insert(customer_info)
            sys.exit('Customer added succesfully')
        except ValueError as e:
            print("Error adding customer:", e)
    if args.method == "get_customer":
        customer_manager = CustomerManager()
        try:
            customer_id = customer_manager.get_customer(id= args.id)
            print("Customer retrieved:", customer_id)
            sys.exit('Customer retrieved succesfully') ### To-do: Add option if id doesn't exist in database.
        except ValueError as e:
            print("Error retrieving customer:", e)
    if args.method == "dump":
        customer_manager = CustomerManager()
        try:
            customer_id = customer_manager.dump(db)
            sys.exit('dumped succesfully')
        except ValueError as e:
            print("Error dumping:", e)
    else:
        print("Invalid method specified.")

if __name__ == "__main__":
    main()