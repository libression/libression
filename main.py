import logging
from typing import Optional

from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi import responses

from file.organiser import Organiser
import uvicorn

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

file_organiser = Organiser()
templates = Jinja2Templates(directory="templates")
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")



@app.get("/")
@app.post("/")
async def index(request: Request):
    return await render_navigation_with_path(request)


@app.get("/thumbnail/{s3_key:path}")
def get_thumbnail(s3_key: str) -> responses.StreamingResponse:
    contents = file_organiser.load_from_cache(s3_key)
    logging.info(f"thumbnail for {s3_key} fetched")
    output = responses.StreamingResponse(contents)
    return output


@app.get("/media/{s3_key:path}")
async def get_resource(s3_key: str):
    logging.info(f"processing media for {s3_key}")
    s3_object = file_organiser.load_from_data_bucket(s3_key)
    logging.info(f"media for {s3_key} processed")
    return responses.FileResponse(s3_object["Body"])


@app.get('/storage/{rel_dir_no_leading_slash:path}', response_class=responses.HTMLResponse)
@app.post('/storage/{rel_dir_no_leading_slash:path}', response_class=responses.HTMLResponse)
async def render_navigation_with_path(
        request: Request,
        rel_dir_no_leading_slash: str = "",
        show_hidden_content: bool = False,
        get_subdir_content: Optional[str] = Form(default=None),
):
    if get_subdir_content is None:
        get_subdir_content = False  # TODO if request.form.get('show_subfolder_content') else False
    else:
        get_subdir_content = True

    nav_dirs, file_keys = file_organiser.get_rel_dirs_and_content(
        rel_dir_no_leading_slash=rel_dir_no_leading_slash,
        get_subdir_content=get_subdir_content,
        show_hidden_content=show_hidden_content,
    )

    file_organiser.ensure_cache_bulk(file_keys)

    template_params = dict(
        request=request,
        keys=file_keys,
        nav_dirs=nav_dirs,
        cur_dir=rel_dir_no_leading_slash or ".",
        get_subdir_content=get_subdir_content,
    )

    return templates.TemplateResponse(
        "navigator.html",
        context=template_params,
    )


"""
# draft function to update file (metadata? tags? dates? whatever)
@app.route('/update', methods=['POST'])
async def update():
    selection = request.form.getlist("impression")
    return render_template(
        'update.html',
        selection=selection,
    )
"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
