import logging
import http
from fastapi import FastAPI, Request, Response, responses
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from libression import organiser, page_entities
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
def single_web_entrypoint(request: Request) -> responses.Response:
    return templates.TemplateResponse("base.html", {"request": request})


@app.post("/refresh_page_params")
def refresh_page_params(
    request: page_entities.PageParamsRequest
) -> page_entities.PageParamsResponse:

    page_metadata = organiser.fetch_page_params(request)
    organiser.update_caches(page_metadata.file_keys)
    return page_metadata


@app.get("/thumbnail/{s3_key:path}")
def thumbnail(s3_key: str) -> responses.StreamingResponse:
    try:
        contents = organiser.load_cache(s3_key)
        status_code = http.HTTPStatus.OK
    except:
        contents = iter([])
        status_code = http.HTTPStatus.NO_CONTENT
    return responses.StreamingResponse(contents, status_code=status_code)


@app.get("/media/{s3_key:path}")
def media(s3_key: str) -> responses.FileResponse:
    content = organiser.get_content(s3_key)
    logging.info(f"media for {s3_key} processed")
    return responses.FileResponse(content)


@app.get("/download/{s3_key:path}")
def download(s3_key: str) -> Response:
    content = organiser.get_content(s3_key)
    logging.info(f"media for {s3_key} processed")
    return Response(content.read())


@app.post("/file_action")
def file_action(
    request: page_entities.FileActionRequest
) -> page_entities.FileActionResponse:

    if request.action == page_entities.FileAction.copy:
        organiser.copy(request.file_keys, request.target_dir)
    elif request.action == page_entities.FileAction.move:
        organiser.move(request.file_keys, request.target_dir)
    elif request.action == page_entities.FileAction.delete:
        organiser.delete(request.file_keys)
    return page_entities.FileActionResponse(success=True)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
