#import fire
import argparse
from pydantic import BaseModel, EmailStr
from tinydb import TinyDB

db = TinyDB('db.json')

class Customer(BaseModel):
    name: str
    email: EmailStr
    age: int
    country: str

class CustomerManager:
    def __init__(self):
        self.customers = []
        self.id = self.get_id()
        self.db = db

    def add_customer(self, name: str, email: str, age: int, country: str):
        customer = Customer(name=name, email=email, age=age, country=country)
        self.customers.append(customer.dict())
        customer_dict = customer.dict()
        customer_dict['id'] = self.id
        return customer_dict
    def get_id(self):
        last_row = db.get(doc_id=len(db))
        if last_row:
            self.id = last_row.doc_id + 1
        else:
            self.id = 1
        return self.id
    def get_customer(self, id):
        User = Query()
        customer = self.db.search(User.id == id)
        return(customer)
def main():
    parser = argparse.ArgumentParser(description="Inserts a customer.")
    parser.add_argument("method", choices=["add_customer"], help="Method to run")
    parser.add_argument("--name", type=str, help="Name", required=True)
    parser.add_argument("--email", type=str, help="Email", required=True)
    parser.add_argument("--age", type=int, help="Age", required=True)
    parser.add_argument("--country", type=str, help="Country", required=True)
    args = parser.parse_args()

    if args.method == "add_customer":
        customer_manager = CustomerManager()
        try:
            customer_info = customer_manager.add_customer(name=args.name, email=args.email, age=args.age,
                                                           country=args.country)
            print("Customer added:", customer_info)
            db.insert(customer_info)
        except ValueError as e:
            print("Error adding customer:", e)
    else:
        print("Invalid method specified.")

if __name__ == "__main__":
    main()
    #fire.fire()

