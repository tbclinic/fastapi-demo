import json
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from time import time
import uvicorn
from sqlalchemy.exc import SQLAlchemyError

from database import create_db, Listing, session

app = FastAPI()

url = 'https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip'
query_parameters = {"downloadformat": "csv"}


@app.get("/")
async def index():
    return {"Hello": "World"}


@app.get("/download")
async def download():
    start = time()
    name = create_db(url, query_parameters)
    end = time()
    time_elapsed = round(end - start, 2)
    return {"message": "Download completed.", "time": time_elapsed, "file_name": name}


@app.get("/{code}")
async def read_item(code: str):
    try:
        results = session.query(Listing).filter_by(zipcode=code).all()
        if not results:
            raise HTTPException(status_code=404, detail="Item not found")
        return jsonable_encoder([result.to_dict() for result in results])
    except SQLAlchemyError as e:
        detail = str(e.__dict__['orig']) if hasattr(e, 'orig') else str(e)
        raise HTTPException(status_code=500, detail=detail)



if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5001)
