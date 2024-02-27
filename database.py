import csv
import os
from python_settings import settings
from pymongo.mongo_client import MongoClient
from download import download_zip
import certifi
from bson.json_util import dumps

os.environ["SETTINGS_MODULE"] = 'settings'

uri = settings.URI
client = MongoClient(uri, tlsCAFile=certifi.where())
db = client['database']
collection = db['listings']


def prepare_listing(row):
    return {
        "zipcode": row[2].zfill(7),
        "state": row[6],
        "city": row[7],
        "address": row[8]
    }


def mongodb(url, query_parameters):
    name = download_zip(url, query_parameters)
    print("Downloading zip file...")

    with open(f"{name}", encoding='utf-8', newline='') as csv_file:
        csv_reader = csv.reader(csv_file, quotechar='"')
        for row in csv_reader:
            if len(row) > 8:  # Ensure the row has enough elements
                listing = prepare_listing(row)
                collection.insert_one(listing)  # Insert the document into MongoDB
    return name


def query(myquery):
    cursor = collection.find(myquery)
    list_cur = list(cursor)
    json_result = dumps(list_cur)
    return json_result
