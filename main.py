from modal import Stub, asgi_app, Image
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from time import time
import uvicorn
from database import mongodb



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
    name = mongodb(url, query_parameters)
    end = time()
    time_elapsed = round(end - start, 2)
    return {"message": "Download completed.", "time": time_elapsed, "file_name": name}


# @app.get("/{code}")
# async def read_item(code: str):
#     try:
#         results = session.query(Listing).filter_by(zipcode=code).all()
#         if not results:
#             raise HTTPException(status_code=404, detail="Item not found")
#         return jsonable_encoder([result.to_dict() for result in results])
#     except SQLAlchemyError as e:
#         detail = str(e.__dict__['orig']) if hasattr(e, 'orig') else str(e)
#         raise HTTPException(status_code=500, detail=detail)


@stub.function(image=image)
@asgi_app()
def fastapi_app():
    return app


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5001)
