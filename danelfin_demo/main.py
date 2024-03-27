import os
import sys
from pydantic import BaseModel, EmailStr # BaseModel for pydantic to understand the format we should introduce the data, EmailStr to validate the e-mail
from tinydb import TinyDB, Query #Importing TinyDB for the creation of the database and Query to use in the "get_customer" method.
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
import fire 

db = TinyDB('db.json') # declaring db variable.

class Customer(BaseModel): # Creation of customer class for add_customer
    name: str
    email: EmailStr
    age: int
    country: str

class CustomerManager: # Customer manager class will handle the methods.

    def __init__(self):
        self.id = self.get_id() # will update id for it's output.
        self.db = db # introducing variable for get_id method
        
    def get_id(self): #Retrieves the len of the database to get it's last ID. Used to add id in add_customer method
        last_row = db.get(doc_id=len(db))
        if last_row:
            self.id = last_row.doc_id + 1
        else:
            self.id = 1
        #logger.info(f' get_id method ran successfully with return : {self.id}') -
        return self.id

    def add_customer(self,name: str, email: str, age: int, country: str): # Retrieves customer from BaseModel for e-mail validation and inserts into the database
        customer = Customer(name=name, email=email, age=age, country=country) # Creates object from Customer, using pydantic validates the entries like e-mail.
        customer_dict = customer.dict() # transforms the customer into a dictionary for easier handling
        customer_dict['id'] = self.id # Adds the column id to the output
        logger.info(f' add_customer method ran successfully with return: {customer_dict}')
        return db.insert(customer_dict) #Method inserts the customer into the database.

    

    def get_client(self, id): # Retrieves the customer from given ID
        User = Query() 
        customer = self.db.search(User.id == id) # Locates whole customer via the ID query.
        if (len(customer) == 0): # if the query returns empty. Warning error appears.
            logger.warning(f"This is a warning message, entry has {len(customer)} values")
            sys.exit('Exiting program, id retrieval unsuccessful')
        customer_dict = dict(customer[0])
        logger.info(f' Customer method ran successfully with id {id} queried: {customer[0]}')
        return customer_dict

    def dump(self):  # Dumps a parquet file partitioned by country .
        logger.debug(f"dump is being initiated")
        pd_dataset = pd.DataFrame(db.all())  # convert db.all to a pandas dataset
        logger.debug(f"len pd dataset:{len(pd_dataset)}")
        if (len(pd_dataset) == 0):
            logger.warning(f"This is a warning message, db.json has {len(pd_dataset)} values")
            sys.exit('Exiting program, dump unsuccessful')
        else:
            pa_dataset = pa.Table.from_pandas(pd_dataset)  # convert pandas dataset to pyarrow dataset
            my_schema = pa.schema([('name', pa.string()),
                                   ('email', pa.string()),
                                   ('age', pa.int64()),
                                   ('country', pa.string()),
                                   ('id', pa.int64())])  # Define the schema for the dump
            partition_scheme = ds.partitioning(pa.schema([pa.field('country', pa.string())]),
                                               flavor=None)  # Define partitioning object
            parquet_file = pq.write_to_dataset(table=pa_dataset, root_path='dump',basename_template='dump{i}.parquet', partitioning=partition_scheme,
                                               schema=my_schema, 
                                               existing_data_behavior='overwrite_or_ignore')
            logger.info(f' database dumped successfully in {pd_dataset.country.unique()} folders')
            
if __name__ == '__main__':
    entry = CustomerManager()
    fire.Fire(entry)

