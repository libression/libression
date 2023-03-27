import logging

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi import responses

from libression import manager
import uvicorn

from libression.manager import update_caches, PageParamsRequest, PageParamsResponse

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
manager.init_buckets()


@app.get("/", response_class=responses.HTMLResponse)
def single_web_entrypoint(request: Request) -> responses.Response:
    return templates.TemplateResponse("base.html", {"request": request})


@app.post("/refresh_page_params")
def refresh_page_params(request: PageParamsRequest) -> PageParamsResponse:
    page_metadata = manager.fetch_page_params(request)
    update_caches(page_metadata.file_keys)
    return page_metadata


@app.get("/thumbnail/{s3_key:path}")
def get_thumbnail(s3_key: str) -> responses.StreamingResponse:
    contents = manager.from_cache(s3_key)
    logging.info(f"thumbnail for {s3_key} fetched")
    return responses.StreamingResponse(contents)


@app.get("/media/{s3_key:path}")
def get_resource(s3_key: str) -> responses.FileResponse:
    s3_object = manager.get(s3_key)
    logging.info(f"media for {s3_key} processed")
    return responses.FileResponse(s3_object["Body"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
