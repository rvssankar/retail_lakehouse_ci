# data_generator/batch_customer_generator.py
"""
Simulates a daily customer master extract (like a source system dump).
Produces day-wise CSV snapshots with realistic inserts + updates so SCD2 has something to do.

Usage:
    python batch_customer_generator.py --day 1 --new 200 --out ./output
    python batch_customer_generator.py --day 2 --new 50 --changes 40 --out ./output
    python batch_customer_generator.py --day 3 --new 50 --changes 60 --out ./output
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime,timezone
from faker import Faker

fake =Faker()
STATE_FILE = "customer_state.json"


def load_state(out_dir):
    path =os.path.join(out_dir,STATE_FILE)

    #print(f"The path created here is {path}")

    if os.path.exists(path):
        with open(path,'r') as f:
            return json.load(f)
    return {}


def save_state(out_dir, state):
    with open(os.path.join(out_dir,STATE_FILE),"w") as f:
        json.dump(state,f,indent=2)


def make_customer(cust_id):
    return{
        "customer_id": str(cust_id),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "segment": random.choice(["Bronze","Silver","Gold"]),
        "source_updated_at": datetime.now(timezone.utc).isoformat(),
    }

def mutate_customer(cust):
    field = random.choice(["address", "city", "state", "zip_code", "phone", "segment", "email"])
    cust =dict(cust)
    if field == "address":
        cust["address"] = fake.street_address()
    elif field == "city":
        cust["city"] = fake.city()
    elif field == "state":
        cust["state"] = fake.state_abbr()
    elif field == "zip_code":
        cust["zip_code"] = fake.zipcode()
    elif field == "phone":
        cust["phone_number"] = fake.phone_number()
    elif field == "segment":
        cust["segment"] = random.choice(["Bronze", "Silver", "Gold"])
    elif field == "email":
        cust["email"] = fake.email()
    cust["source_updated_at"] = datetime.now(timezone.utc).isoformat()

    return cust


def main():
    parser =argparse.ArgumentParser()
    parser.add_argument("--day",type=int,required=True)
    parser.add_argument("--new",type=int, default=100)
    parser.add_argument("--changes",type=int, default=0)
    parser.add_argument("--out",type=str,default="./output")
    args = parser.parse_args()
  

    path = os.path.abspath(os.path.join(os.path.dirname(__file__),args.out))
    os.makedirs(path,exist_ok=True)
        
    state =load_state(path)


    existing_ids =list(state.keys())

    next_id = max([int(i) for i in existing_ids],default=1000) +1

    rows =[]

    if args.changes and existing_ids:
        change_ids = random.sample(existing_ids,min(args.changes,len(existing_ids)))

        for cid in change_ids:
            updated =mutate_customer(state[cid])
            state[cid]=updated
            rows.append(updated)


    for _ in range(args.new):
        cust =make_customer(next_id)
        state[str(next_id)] =cust
        rows.append(cust)
        next_id +=1

    save_state(path,state)

    day_dir = os.path.join(path,f"day{args.day}")
    os.makedirs(day_dir,exist_ok=True)
    out_file =os.path.join(day_dir,"customers.csv")


    fieldnames = [
        "customer_id", "first_name", "last_name", "email", "phone_number",
        "address", "city", "state", "zip_code", "segment", "source_updated_at",
    ]

    with open(out_file,"w",newline="") as f:
        writer =csv.DictWriter(f,fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


    print(f"Day {args.day}: wrote {len(rows)} rows ({args.changes} changed , {args.new} new ) -> {out_file}")




if __name__ == "__main__":
    main()
    