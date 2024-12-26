import enum
import uuid
import datetime
import dataclasses
import array
import typing

@dataclasses.dataclass
class TagBitSet:
    bits: array.array

    @classmethod
    def new(cls) -> 'TagBitSet':
        bits = array.array('Q')
        while len(bits) * 64 < 256:  # Pre-allocate space for 256 tags
            bits.append(0)
        return cls(bits)

    @classmethod
    def from_blob(cls, blob: bytes | None) -> 'TagBitSet':
        bits = array.array('Q')
        if blob:
            bits.frombytes(blob)
        while len(bits) * 64 < 256:  # Ensure minimum size
            bits.append(0)
        return cls(bits)

    def add_tag(self, tag_id: int) -> None:
        array_idx = tag_id // 64
        bit_idx = tag_id % 64
        while array_idx >= len(self.bits):
            self.bits.append(0)  # expand space as needed
        self.bits[array_idx] |= (1 << bit_idx)

    def remove_tag(self, tag_id: int) -> None:
        array_idx = tag_id // 64
        bit_idx = tag_id % 64
        if array_idx < len(self.bits):
            self.bits[array_idx] &= ~(1 << bit_idx)

    def has_tag(self, tag_id: int) -> bool:
        array_idx = tag_id // 64
        bit_idx = tag_id % 64
        if array_idx >= len(self.bits):
            return False
        return bool(self.bits[array_idx] & (1 << bit_idx))

    def to_blob(self) -> bytes:
        return self.bits.tobytes()


class DBFileAction(enum.Enum):
    """
    All actions that can be performed on a file
    """
    CREATE = 'CREATE'  # should always be first action
    MOVE = 'MOVE'
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'  # changes (e.g. rotation or photoshop, can be libression/external)


class DBFileEntry(typing.NamedTuple):
    file_key: str
    thumbnail_key: str | None
    thumbnail_checksum: str | None
    thumbnail_phash: str | None
    action_type: DBFileAction
    file_entity_uuid: str
    mime_type: str | None

    # Auto-generated fields
    table_id: int | None = None 
    created_at: datetime.datetime | None = None


def new_db_file_entry(
    file_key: str,
    thumbnail_key: str | None = None,
    thumbnail_checksum: str | None = None,
    thumbnail_phash: str | None = None,
    mime_type: str | None = None,
) -> 'DBFileEntry':
    return DBFileEntry(
        file_key=file_key,
        thumbnail_key=thumbnail_key,
        thumbnail_checksum=thumbnail_checksum,
        thumbnail_phash=thumbnail_phash,
        action_type=DBFileAction.CREATE,
        file_entity_uuid=str(uuid.uuid4()),
        mime_type=mime_type,
        table_id=None,  # explicitly None
        created_at=None,  # explicitly None
    )

def existing_db_file_entry(
    file_key: str,
    thumbnail_key: str,
    thumbnail_checksum: str | None,
    thumbnail_phash: str | None,
    action_type: DBFileAction,
    file_entity_uuid: str,
    mime_type: str | None = None,
) -> 'DBFileEntry':
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
        table_id=None,  # explicitly None
        created_at=None,  # explicitly None
    )
