import array
import dataclasses
import datetime
import enum
import sqlite3  # Just for type hints. NOT for any operations!
import sys
import typing
import uuid


@dataclasses.dataclass
class TagBitSet:
    """
    A set of tags represented as a bit array for database storage.
    Handles conversion between tag IDs and blob format.
    """

    bits: array.array

    @classmethod
    def from_tag_ids(cls, tag_ids: list[int]) -> "TagBitSet":
        """Create TagBitSet from a list of tag IDs."""
        if not tag_ids:
            return cls(array.array("Q"))

        # Find required array size
        max_array_idx = max(tag_id // 64 for tag_id in tag_ids)
        bits = array.array("Q", [0] * (max_array_idx + 1))

        # Set bits for each tag ID
        for tag_id in tag_ids:
            array_idx = tag_id // 64
            bit_idx = tag_id % 64
            bits[array_idx] |= 1 << bit_idx

        return cls(bits)

    @classmethod
    def from_blob(cls, blob: bytes) -> "TagBitSet":
        """Create TagBitSet from a blob."""
        bits = array.array("Q")
        if blob:
            # Force little-endian byte order for cross-platform compatibility
            if sys.byteorder == "big":
                blob = bytes(reversed(blob))
            bits.frombytes(blob)
            if len(blob) % 8 != 0:
                raise ValueError("Invalid blob length for tag bits")
        return cls(bits)

    def get_tag_ids(self) -> list[int]:
        """Get list of set tag IDs."""
        tag_ids = []
        for array_idx, value in enumerate(self.bits):
            if value:  # Skip empty arrays
                base = array_idx * 64
                for bit_idx in range(64):
                    if value & (1 << bit_idx):
                        tag_ids.append(base + bit_idx)
        return tag_ids

    def to_blob(self) -> bytes:
        # Force little-endian byte order for cross-platform compatibility
        if sys.byteorder == "big":
            return bytes(reversed(self.bits.tobytes()))
        return self.bits.tobytes()

    def __eq__(self, other: object) -> bool:
        """Compare two TagBitSets."""
        if not isinstance(other, TagBitSet):
            return NotImplemented
        return self.to_blob() == other.to_blob()

    def __repr__(self) -> str:
        """Show tag IDs for debugging."""
        return f"TagBitSet(tag_ids={self.get_tag_ids()})"


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

    def get_ids(self, names: typing.Sequence[str]) -> list[int | None]:
        """Get multiple tag IDs from names."""
        return [self.name_to_id.get(name) for name in names]

    def get_names(self, tag_ids: typing.Sequence[int]) -> list[str | None]:
        """Get multiple tag names from IDs."""
        return [self.id_to_name.get(tag_id) for tag_id in tag_ids]

    def __contains__(self, value: str | int) -> bool:
        """Check if name or ID exists in mapping."""
        if isinstance(value, str):
            return value in self.name_to_id
        if isinstance(value, int):
            return value in self.id_to_name
        raise ValueError(f"Invalid input value type: {type(value)}")


class DBFileAction(enum.Enum):
    """
    All actions that can be performed on a file
    """

    CREATE = "CREATE"  # should always be first action
    MOVE = "MOVE"
    DELETE = "DELETE"
    UPDATE = (
        "UPDATE"  # changes (e.g. rotation or photoshop, can be libression/external)
    )
    # UPDATE also covers tag changes (doesn't go into file_action, just within function)


class DBFileEntry(typing.NamedTuple):
    file_key: str
    file_entity_uuid: str
    action_type: DBFileAction

    # Optional fields
    mime_type: str | None = None
    thumbnail_key: str | None = None
    thumbnail_checksum: str | None = None
    thumbnail_phash: str | None = None
    tags: typing.Sequence[str] = tuple()  # converts to tag_bits in db

    # Auto-generated fields
    table_id: int | None = None
    created_at: datetime.datetime | None = None


def new_db_file_entry(
    file_key: str,
    thumbnail_key: str | None = None,
    thumbnail_checksum: str | None = None,
    thumbnail_phash: str | None = None,
    mime_type: str | None = None,
    tags: typing.Sequence[str] = tuple(),
) -> "DBFileEntry":
    """
    Generates an file_entity_id (ready for db insert)
    """
    return DBFileEntry(
        file_key=file_key,
        thumbnail_key=thumbnail_key,
        thumbnail_checksum=thumbnail_checksum,
        thumbnail_phash=thumbnail_phash,
        action_type=DBFileAction.CREATE,
        file_entity_uuid=str(uuid.uuid4()),
        mime_type=mime_type,
        tags=tags,
        table_id=None,  # explicitly None
        created_at=None,  # explicitly None
    )


def existing_db_file_entry(
    file_key: str,
    file_entity_uuid: str,
    action_type: DBFileAction,
    thumbnail_key: str | None = None,
    thumbnail_checksum: str | None = None,
    thumbnail_phash: str | None = None,
    mime_type: str | None = None,
    tags: typing.Sequence[str] = tuple(),
) -> "DBFileEntry":
    """Factory method for actions on existing files."""
    if action_type == DBFileAction.CREATE:
        raise ValueError("Use create() for new files")

    return DBFileEntry(
        file_key=file_key,
        thumbnail_key=thumbnail_key,
        thumbnail_checksum=thumbnail_checksum,
        thumbnail_phash=thumbnail_phash,
        action_type=action_type,
        file_entity_uuid=file_entity_uuid,
        mime_type=mime_type,
        tags=tags,
        table_id=None,  # explicitly None
        created_at=None,  # explicitly None
    )
