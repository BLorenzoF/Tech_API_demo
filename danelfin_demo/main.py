import os
import sys
from pydantic import BaseModel, EmailStr # BaseModel for pydantic , EmailStr to validate the e-mail
from tinydb import TinyDB, Query #TinyDB with Query for the creation of the database and handling
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
import fire 


class Customer(BaseModel): # Creation of customer class for add_customer
    logger.info('creating Pydantic customer basemodel ')
    name: str
    email: EmailStr
    age: int
    country: str

class CustomerManager: # Customer manager class will handle the methods.
    
    def __init__(self):
        #db = TinyDB('db.json') 
        self.db = TinyDB('db.json') # introducing variable for get_id method
        self.id = self.get_id() # will update id for it's output.
        self.dump_path = 'dump' # Folder where parquet files will be dumped
        
        
    def get_id(self): #Retrieves the len of the database to get it's last ID. Used to add id in add_customer method
        last_row = self.db.get(doc_id=len(self.db))
        if last_row:
            self.id = last_row.doc_id + 1
        else:
            self.id = 1
        logger.info(f' get_id method ran successfully with return : {self.id}')
        return self.id

    def add_customer(self,name: str, email: str, age: int, country: str): # Retrieves customer from BaseModel for e-mail validation and inserts into the database
        '''Inserts given customer data into db.json database, given values checked by pydantic

    	Parameters:
        --name (str): Name of the customer
        
        --email (str): e-mail of the customer, required to have a "@" symbol and a domain extension (.com, .org ...)
        
        --age (int): age of the customer
        
        --country (str): Country of the customer. This parameter will be used for partitioning when using "dump" method 

    	Returns:
    	
        id number of newly inserted object at the database   
    	'''
        customer = Customer(name=name, email=email, age=age, country=country) # Creates object from Customer.
        customer_dict = customer.dict() # transforms the customer into a dictionary for easier handling
        customer_dict['id'] = self.id # Adds the column id to the output
        logger.info(f' add_customer method ran successfully with return: {customer_dict}')
        return self.db.insert(customer_dict) #Method inserts the customer into the database.

    def get_client(self, id): # Retrieves the customer from given ID
        '''Retrieves customer data from given ID
	
    	Parameters:
        --id (int): id of customer in the db.json file
	
    	Returns:
    	name
    	email
    	age
    	country
    	id
    	'''
        User = Query() #Instance of query into the database
        client_query = self.db.search(User.id == id) # Locates whole customer via the ID query.
        if (len(client_query) == 0): # if the query returns empty. Warning error appears.
            logger.warning(f"This is a warning message, entry has {len(client_query)} values")
            sys.exit('Exiting program, id retrieval unsuccessful')
        else:
            logger.info(f' Customer method ran successfully with id {id} queried: {client_query[0]}')
            customer_dict = dict(client_query[0])
        
            return customer_dict

    def dump(self):  # Dumps a parquet file partitioned by country .
        '''dumps the db.json in files partitioned by country
        
    	Parameters:
        none
        
    	Returns:
    	parquet files at folder "dump/{country}/dump{i}.parquet"
    	'''
    	
        #logger.info("dump is being initiated")
        pd_dataset = pd.DataFrame(self.db.all())  # convert db.all to a pandas dataset
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
            parquet_file = pq.write_to_dataset(table=pa_dataset, root_path=self.dump_path,basename_template='dump{i}.parquet', partitioning=partition_scheme,
                                               schema=my_schema, 
                                               existing_data_behavior='overwrite_or_ignore')
            logger.info(f' database dumped successfully in {pd_dataset.country.unique()} folders')

if __name__ == '__main__':
    entry = CustomerManager()
    fire.Fire(entry)

