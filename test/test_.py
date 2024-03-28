import os
from os import path
import sys
import shutil
from danelfin_demo.main import CustomerManager
# Add the src directory to the path
#os.environ["JUPYTER_PLATFORM_DIRS"] = "1"
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "danelfin_demo"))
)
import pytest
from pydantic import EmailStr
from tinydb import TinyDB
from loguru import logger

db = TinyDB('dbtest.json')

def test_add_customer(): # Creates a new database called dbtest.json and inserts a correct value
    customer = CustomerManager()
    customer.db = TinyDB('dbtest.json')
    customer.add_customer(name= 'name', email='email@age.com', age=22, country='country_test')
    assert len(customer.db.all()) == 1

def test_get_client(): # Checks db.json for it's last value
    client = CustomerManager()
    db_len = len(client.db.all())
    logger.warning(f'db_len is: {db_len}')
    if (db_len == 0): # if the query returns empty. Warning error appears.
        logger.warning(f"This is a warning message, TEST entry has {len(client.db.search(client.id == id))} values")
        assert (len(client.db.search(client.id == id)) == 0)
    else:
        last_row = client.db.get(doc_id=len(client.db)) # Get's last row of db.json
        last_id = last_row.doc_id # get's id from last_row
    #logger.info(f'row_id is: {last_row.doc_id}')
        client_return = client.get_client(id= last_id)
        logger.info(f' client return is: {client_return}')
        assert client_return['id'] == len(client.db.all())
    

def test_dump():
    customer = CustomerManager()
    customer.db = TinyDB('dbtest.json')
    customer.dump_path = 'test_dump'
    customer.dump()
    parquet_dir= os.getcwd() + '/test_dump'
    parquet_file= os.getcwd() + '/test_dump/country_test/dump0.parquet'
    
    assert path.isdir(parquet_dir) and path.isfile(parquet_file)

if __name__ == '__main__':
    import pytest
    pytest.main(['-v'])
    
    

