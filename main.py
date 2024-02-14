from zipfile import ZipFile, ZipInfo
from fastapi import FastAPI
from time import time
import uvicorn
import csv
import requests
from db import create_db

app = FastAPI()

url = 'https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip'
query_parameters = {"downloadformat": "csv"}
result = []


def download_zip(link, parameters):
    file_name = "postalcode"
    response = requests.get(link, params=parameters)
    with open(f"{file_name}.zip", mode="wb") as file:
        file.write(response.content)
        file.close()
    with ZipFile(f"{file_name}.zip", 'r') as zip:
        zipinfo = zip.infolist()
        for item in zipinfo:
            if item.filename.endswith('.csv'):
                csv_file_name = item.filename
                break

        if csv_file_name is not None:
            print('Extracting all the files now...')
            zip.extractall()  # Extracting all files
            print('Done!')
        else:
            print("No CSV file found in the ZIP.")
    return csv_file_name


@app.get("/")
def index():
    return {"Hello": "World"}


@app.get("/download")
def download():
    start = time()
    name = create_db(url, query_parameters)
    end = time()
    time_elapsed = round(end - start, 2)
    return {"message": "Download completed.", "time": time_elapsed, "file_name": name}


# @app.get("/{code}")
# def read_item(code: int, filename=csv_file_name):
#     with open(filename, newline='') as f:
#         reader = csv.DictReader(f, delimiter=',')
#         for row in reader:
#             for field in row:
#                 if field == code:
#                     print("True")
#                     result.append(row)
#     return {"Postal Code ": code, "Result ": result}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5001)
