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

db = TinyDB('db.json')

def test_add_customer():
    customer = CustomerManager()
    customer.add_customer(name= 'name', email='email@age.com', age=22, country='country_test')
    assert len(db.all()) == 1

def test_get_customer():
    customer = CustomerManager()
    client = customer.get_client(id= 1)
    assert client['id'] == len(db.all())
    

def test_dump():
    customer = CustomerManager()
    customer.dump()
    parquet_dir= os.getcwd() + '/dump'
    parquet_file= os.getcwd() + '/dump/country_test/dump0.parquet'
    
    assert path.isdir(parquet_dir) and path.isfile(parquet_file)

if __name__ == '__main__':
    import pytest
    pytest.main(['-v'])
    
    

