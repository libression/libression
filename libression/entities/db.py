import dataclasses
import datetime
import enum
import sqlite3  # Just for type hints. NOT for any operations!
import typing
import uuid
import pydantic
import urllib.parse


@dataclasses.dataclass
class TagMapping:
    """
    Bidirectional mapping for tag names and IDs
    Always fetch from db (even after inserts)
    Makes it more human readable and easier to use
    """

    name_to_id: dict[str, int]
    id_to_name: dict[int, str]

    @classmethod
    def from_rows(cls, rows: typing.Sequence[sqlite3.Row]) -> "TagMapping":
        """Create mapping from database rows."""
        name_to_id = {}
        id_to_name = {}
        for row in rows:
            name_to_id[row["name"]] = row["id"]
            id_to_name[row["id"]] = row["name"]
        return cls(name_to_id, id_to_name)


class DBFileAction(enum.Enum):
    """
    All actions that can be performed on a file

    UPDATE includes:
    - file changes (e.g. rotation or photoshop, can be libression/external)
    - tag changes (doesn't go into file_action, just within function)
    """

    CREATE = "CREATE"  # should always be first action (also for copies)
    MOVE = "MOVE"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    MISSING = (
        "MISSING"  # Treat as DELETE-ish (but different, so we can tell them apart)
    )


class DBFileEntry(typing.NamedTuple):
    """
    Main object for file/tag operations combined
    """

    # Required fields
    file_key: str
    file_entity_uuid: str
    action_type: DBFileAction

    # Optional fields
    mime_type: str | None = None
    thumbnail_key: str | None = None
    thumbnail_mime_type: str | None = None
    thumbnail_checksum: str | None = None
    thumbnail_phash: str | None = None
    tags: typing.Sequence[str] = tuple()  # converts to tag_bits in db

    # System-generated fields
    action_created_at: datetime.datetime | None = None  # by db, not user
    tags_created_at: datetime.datetime | None = None  # by service, not user

    @classmethod
    def from_dict(cls, data: dict):
        # Extract only the fields we need
        fields = {k: data[k] for k in cls._fields if k in data}
        return cls(**fields)

    def to_dict(self) -> dict:
        """Convert the DBFileEntry object to a dictionary."""
        return {
            "file_key": self.file_key,
            "file_entity_uuid": self.file_entity_uuid,
            "thumbnail_key": self.thumbnail_key,
            "thumbnail_mime_type": self.thumbnail_mime_type,
            "thumbnail_checksum": self.thumbnail_checksum,
            "thumbnail_phash": self.thumbnail_phash,
            "mime_type": self.mime_type,
            "tags": self.tags,
            # Add any other fields that should be included in the response
        }


def new_db_file_entry(
    file_key: str,
    thumbnail_key: str | None = None,
    thumbnail_mime_type: str | None = None,
    thumbnail_checksum: str | None = None,
    thumbnail_phash: str | None = None,
    mime_type: str | None = None,
    tags: typing.Sequence[str] = tuple(),
) -> DBFileEntry:
    """
    Generates an file_entity_id (ready for db insert)
    """
    # force file_key to be unquoted
    file_key = urllib.parse.unquote(file_key)

    return DBFileEntry(
        file_key=file_key,
        thumbnail_key=thumbnail_key,
        thumbnail_mime_type=thumbnail_mime_type,
        thumbnail_checksum=thumbnail_checksum,
        thumbnail_phash=thumbnail_phash,
        action_type=DBFileAction.CREATE,
        file_entity_uuid=str(uuid.uuid4()),
        mime_type=mime_type,
        tags=tags,
        action_created_at=None,  # explicitly None
        tags_created_at=None,  # explicitly None
    )


def existing_db_file_entry(
    file_key: str,
    file_entity_uuid: str,
    action_type: DBFileAction,
    thumbnail_key: str | None = None,
    thumbnail_mime_type: str | None = None,
    thumbnail_checksum: str | None = None,
    thumbnail_phash: str | None = None,
    mime_type: str | None = None,
    tags: typing.Sequence[str] = tuple(),
) -> DBFileEntry:
    """Factory method for actions on existing files."""
    if action_type == DBFileAction.CREATE:
        raise ValueError("Use create() for new files")

    # force file_key to be unquoted
    file_key = urllib.parse.unquote(file_key)

    return DBFileEntry(
        file_key=file_key,
        thumbnail_key=thumbnail_key,
        thumbnail_mime_type=thumbnail_mime_type,
        thumbnail_checksum=thumbnail_checksum,
        thumbnail_phash=thumbnail_phash,
        action_type=action_type,
        file_entity_uuid=file_entity_uuid,
        mime_type=mime_type,
        tags=tags,
        action_created_at=None,  # explicitly None
        tags_created_at=None,  # explicitly None
    )


class DBTagEntry(pydantic.BaseModel):
    """
    Minimal info required for tag operations (subset of DBFileEntry)
    """

    file_entity_uuid: str
    tags: typing.Sequence[str] = tuple()  # converts to tag_bits in db
