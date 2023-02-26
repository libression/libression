import logging

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi import responses

from file import organiser, s3
import uvicorn

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
organiser.init_buckets()


@app.get("/", response_class=responses.HTMLResponse)
async def single_web_entrypoint(request: Request):
    context = await render_navigation_with_path()
    context["request"] = request
    return templates.TemplateResponse("base.html", context=context)


@app.get("/thumbnail/{s3_key:path}")
def get_thumbnail(s3_key: str) -> responses.StreamingResponse:
    contents = organiser.load_from_cache(s3_key)
    logging.info(f"thumbnail for {s3_key} fetched")
    output = responses.StreamingResponse(contents)
    return output


@app.get("/media/{s3_key:path}")
async def get_resource(s3_key: str):
    logging.info(f"processing media for {s3_key}")
    s3_object = s3.get_object(key=s3_key, bucket_name=organiser.DATA_BUCKET)
    logging.info(f"media for {s3_key} processed")
    return responses.FileResponse(s3_object["Body"])


async def render_navigation_with_path() -> dict:

    nav_dirs, file_keys = organiser.get_rel_dirs_and_content(
        rel_dir_no_leading_slash="",
        get_subdir_content=False,
        show_hidden_content=False,
    )

    organiser.ensure_cache_bulk(file_keys)

    return dict(
        keys=file_keys,
        nav_dirs=nav_dirs,
        cur_dir=".",
        get_subdir_content=False,
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
