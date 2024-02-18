from enum import Enum
from typing import Optional, Iterable
from pydantic import BaseModel


class PageParamsRequest(BaseModel):
    cur_dir: str
    show_subdirs: bool
    show_hidden_content: bool
    # active_tags: Iterable[str] = tuple()
    # supressed_tags: Iterable[str] = tuple()


class PageParamsResponse(BaseModel):
    file_keys: list[str]
    inner_dirs: list[str]
    par_dir: str


class FileAction(Enum):
    move = "move"
    copy = "copy"
    delete = "delete"

# class DbAction(Enum):
    # assign_tag = "assign_tag"
    # remove tag = "remove_tag"
    #


class FileActionRequest(BaseModel):
    action: FileAction
    file_keys: list[str]
    target_dir: str


class FileActionResponse(BaseModel):
    success: bool
    message: Optional[str] = None
