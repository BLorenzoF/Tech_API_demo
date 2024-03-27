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
        self.customers = []
        self.id = self.get_id() # will update id for it's output.
        self.db = db # introducing variable for get_id method

    def add_customer(self, name: str, email: str, age: int, country: str): # add_customer method. Retrieves customer from BaseModel for e-mail validation
        customer = Customer(name=name, email=email, age=age, country=country)
        self.customers.append(customer.dict())
        customer_dict = customer.dict()
        customer_dict['id'] = self.id # adds
        logger.info(f' add_customer method ran successfully with return: {customer_dict}')
        return db.insert(customer_dict)

    def get_id(self): #method that gets the len of the database to get it's last ID, not fool-proof to removal of rows.
        last_row = db.get(doc_id=len(db))
        if last_row:
            self.id = last_row.doc_id + 1
        else:
            self.id = 1
        #logger.info(f' get_id method ran successfully with return : {self.id}') -
        return self.id

    def get_client(self, id): ##MISSING error if id doesn't exist in database.
        User = Query()
        customer = self.db.search(User.id == id)
        if (len(customer) == 0):
            logger.warning(f"This is a warning message, entry has {len(customer)} values")
            sys.exit('Exiting program, id retrieval unsuccessful')
        customer_dict = dict(customer[0])
        logger.info(f' Customer method ran successfully with id {id} queried: {customer}')
        return print(customer_dict)

    def dump(self):  # method for dumping a parquet file
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
            parquet_file = pq.write_to_dataset(table=pa_dataset, root_path=os.getcwd(), partitioning=partition_scheme,
                                               schema=my_schema, basename_template='dump{i}.parquet',
                                               existing_data_behavior='overwrite_or_ignore')
            logger.info(f' database dumped successfully in {pd_dataset.country.unique()} folders')
            

if __name__ == '__main__':
    entry = CustomerManager()
    fire.Fire(entry)

