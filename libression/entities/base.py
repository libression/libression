import pydantic


class FileActionResponse(pydantic.BaseModel):
    file_key: str
    success: bool
    error: str | None = None


class UploadEntry(pydantic.BaseModel):
    file_source: str = pydantic.Field(
        description="base64 encoded file (later support url?)",
    )
    filename: str = pydantic.Field(
        description="Original filename to use for the key",
    )

    @pydantic.field_validator("filename")
    def validate_filename(cls, v):
        if "%" in v:
            raise ValueError(
                "Filename should be unquoted and not contain percent encoding"
            )
        return v
