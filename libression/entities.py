from enum import Enum
from typing import Optional
from pydantic import BaseModel


class PageParamsRequest(BaseModel):
    cur_dir: str
    show_subdirs: bool
    show_hidden_content: bool


class PageParamsResponse(BaseModel):
    file_keys: list[str]
    inner_dirs: list[str]
    outer_dir: str


class FileAction(Enum):
    move = "move"
    copy = "copy"
    delete = "delete"


class FileActionRequest(BaseModel):
    action: FileAction
    file_keys: list[str]
    target_dir: str


class FileActionResponse(BaseModel):
    success: bool
    message: Optional[str] = None
