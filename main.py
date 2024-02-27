from modal import Stub, asgi_app, Image
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from time import time
import uvicorn
from database import query


stub = Stub("fastapi-demo")
app = FastAPI()
url = 'https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip'
query_parameters = {"downloadformat": "csv"}

image = Image.debian_slim().pip_install_from_requirements("requirements.txt")


@app.get("/")
async def index():
    return {"Hello": "World"}


@app.get("/download")
async def download():
    start = time()
    # name = mongodb(url, query_parameters)
    name = "test"
    end = time()
    time_elapsed = round(end - start, 2)
    return {"message": "Download completed.", "time": time_elapsed, "file_name": name}


@app.get("/{code}")
async def read_item(code: str):
    myquery = {"zipcode": code.zfill(7)}
    results = query(myquery)
    for item in results:
        if '_id' in item:
            item['_id'] = str(item['_id'])
    encoded_json = jsonable_encoder(results)
    return encoded_json


@stub.function(image=image)
@asgi_app()
def fastapi_app():
    return app


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5001)
