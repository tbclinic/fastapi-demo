import csv
import os
from pymongo.server_api import ServerApi
from python_settings import settings
from pymongo.mongo_client import MongoClient
from download import download_zip
import certifi
from bson.json_util import dumps, loads
from pymongo.operations import InsertOne

os.environ["SETTINGS_MODULE"] = 'settings'

uri = settings.URI
client = MongoClient(uri, tlsCAFile=certifi.where(), server_api=ServerApi('1'))
db = client['database']
collection = db['listings']


def prepare_listing(row):
    return {
        "zipcode": row[2].zfill(7),
        "state": row[6],
        "city": row[7],
        "address": row[8]
    }


def insert(url, query_parameters):
    name = download_zip(url, query_parameters)
    print("Downloading zip file...")

    operations = []
    batch_size = 10000

    with open(f"{name}", encoding='utf-8', newline='') as csv_file:
        csv_reader = csv.reader(csv_file, quotechar='"')
        for row in csv_reader:
            if len(row) > 8:
                listing = prepare_listing(row)
                operations.append(InsertOne(listing))

                if len(operations) == batch_size:
                    collection.bulk_write(operations)
                    operations = []
    return name


def query(myquery):
    cursor = collection.find(myquery)
    return loads(dumps(cursor))


def checkdb():
    try:
        client.admin.command('ping')
        print("OK")
        return "OK"
    except Exception as e:
        error_message = str(e)
        print(error_message)
        return error_message


def dropdb():
    try:
        collection.drop()
        msg = f"The collection '{collection.name}' has been dropped."
        print(msg)
        return msg
    except Exception as e:
        msg = f"An error occurred: {e}"
        print(msg)
        return msg
