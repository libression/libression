import datetime
import pathlib
import sqlite3
import typing

import alembic.command
from alembic.config import Config

import libression


class DBClient:
    def __init__(self, db_path: str | pathlib.Path):
        self.db_path = pathlib.Path(db_path)
        self._tag_mapping: libression.entities.db.TagMapping | None = (
            None  # Cache for tag lookups
        )

        # Register datetime converter
        sqlite3.register_converter(
            "datetime", lambda x: datetime.datetime.fromisoformat(x.decode())
        )

        self._ensure_db()

    def _ensure_db(self) -> None:
        """Create database and apply migrations."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Setup Alembic config
        alembic_cfg = Config()
        alembic_cfg.set_main_option(
            "script_location",
            str(pathlib.Path(__file__).parent / "migrations"),
        )
        alembic_cfg.set_main_option(
            "sqlalchemy.url",
            f"sqlite:///{self.db_path}",
        )

        # Run migrations
        alembic.command.upgrade(alembic_cfg, "head")

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection with proper settings."""
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
            | sqlite3.PARSE_COLNAMES,  # Enable type conversion
        )
        conn.row_factory = sqlite3.Row

        # Performance optimizations
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA datetime_precision=6")

        return conn

    ############################################################################################
    # tags and file_tags tables
    ############################################################################################

    def _cache_tag_mapping(
        self, cursor: sqlite3.Cursor, force_update: bool
    ) -> libression.entities.db.TagMapping:
        """
        requires a connected cursor
        syncs tags from the database (registers new tags)
        """
        if self._tag_mapping is None or force_update:
            self._tag_mapping = libression.entities.db.TagMapping.from_rows(
                cursor.execute("SELECT id, name FROM tags").fetchall()
            )
        return self._tag_mapping

    def _sync_tags_by_tag_names(
        self,
        tags: list[str],
        cursor: sqlite3.Cursor,
    ) -> libression.entities.db.TagMapping:
        """
        requires a connected cursor
        syncs tags from the database (registers new tags)
        """

        tag_mapping = self._cache_tag_mapping(
            cursor, force_update=False
        )  # ensures loaded and not None

        tags_with_missing_ids = [
            tag for tag in tags if tag not in tag_mapping.name_to_id.keys()
        ]

        if tags_with_missing_ids:
            # Insert missing tags
            for tag in tags_with_missing_ids:
                cursor.execute("INSERT INTO tags (name) VALUES (?)", (tag,))

            # Update the cache (new ids can't be known ahead of time ... so second db call)
            tag_mapping = self._cache_tag_mapping(cursor, force_update=True)

        return tag_mapping  # reference only...can do copy if needed (safer)

    def _tags_to_bitset(
        self,
        tags: list[str],
        cursor: sqlite3.Cursor,
    ) -> libression.entities.db.TagBitSet | None:
        """
        requires a connected cursor
        only allow Nones in tags in db (not in python entities)
        """
        if not tags:
            return None

        tag_map = self._sync_tags_by_tag_names(tags, cursor)

        tag_ids = [x for x in tag_map.get_ids(tags) if x is not None]

        if any([tag_id is None for tag_id in tag_ids]):
            raise ValueError("tag(s) not registered in db! should not be here...")

        # Create and populate bitset
        return libression.entities.db.TagBitSet.from_tag_ids(tag_ids)

    def _insert_file_tags(
        self,
        entries: list[libression.entities.db.DBFileEntry],
        cursor: sqlite3.Cursor,
    ) -> None:
        """
        requires a connected cursor
        """

        invalid_entries = [entry for entry in entries if not entry.file_entity_uuid]

        if invalid_entries:
            raise ValueError("Entries with no entity_uuid cannot be inserted!")

        # Filter entries with tags and prepare parameters
        tag_params = []
        for entry in entries:
            blob: bytes | None = None
            bitset = self._tags_to_bitset(list(entry.tags), cursor)
            if bitset is not None:
                blob = bitset.to_blob()
            tag_params.append((entry.file_entity_uuid, blob))

        if tag_params:  # Only execute if we have tags to insert
            cursor.executemany(
                "INSERT INTO file_tags (file_entity_uuid, tag_bits) VALUES (?, ?)",
                tag_params,
            )

    def register_file_tags(
        self,
        entries: list[libression.entities.db.DBFileEntry],
    ) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            self._insert_file_tags(entries, cursor)

    ############################################################################################
    # file_action table
    ############################################################################################

    def _insert_file_actions(
        self,
        entries: list[libression.entities.db.DBFileEntry],
        cursor: sqlite3.Cursor,
    ) -> list[tuple[int, datetime.datetime]]:
        """Insert entries into file_actions table and return (id, created_at) pairs."""
        return [
            cursor.execute(
                """
                INSERT INTO file_actions (
                    file_entity_uuid,
                    file_key,
                    action_type,
                    thumbnail_key,
                    thumbnail_checksum,
                    thumbnail_phash,
                    mime_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING id, created_at
                """,
                (
                    entry.file_entity_uuid,
                    entry.file_key,
                    entry.action_type.value,
                    entry.thumbnail_key,
                    entry.thumbnail_checksum,
                    entry.thumbnail_phash,
                    entry.mime_type,
                ),
            ).fetchone()
            for entry in entries
        ]

    def register_files(
        self,
        entries: list[libression.entities.db.DBFileEntry],
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Insert entries into file_actions and file_tags tables.
        Returns the fully populated objects (with ids, timestamps, etc.).
        """
        if not entries:
            return []

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            created_at_ids = self._insert_file_actions(entries, cursor)

            registered_entries = []
            for i, entry in enumerate(entries):
                entry_dict = entry._asdict()
                entry_dict["table_id"] = created_at_ids[i][0]  # Update instead of add
                entry_dict["created_at"] = created_at_ids[i][1]  # Update instead of add
                registered_entries.append(
                    libression.entities.db.DBFileEntry(**entry_dict)
                )

            self._insert_file_tags(registered_entries, cursor)
            return registered_entries

    ############################################################################################
    # query methods
    ############################################################################################

    def _sync_tags_by_tag_ids(
        self,
        tag_ids: list[int],
        cursor: sqlite3.Cursor,
    ) -> libression.entities.db.TagMapping:
        """
        requires a connected cursor
        syncs tags from the database (registers new tags)
        """

        tag_mapping = self._cache_tag_mapping(
            cursor, force_update=False
        )  # ensures loaded and not None

        tag_ids_with_missing_names = [
            tag_id for tag_id in tag_ids if tag_id not in tag_mapping.id_to_name.keys()
        ]

        if tag_ids_with_missing_names:
            tag_mapping = self._cache_tag_mapping(cursor, force_update=True)

        if any(tag_id not in tag_mapping.id_to_name.keys() for tag_id in tag_ids):
            raise ValueError("tag id(s) not registered in db! should not be here...")

        return tag_mapping  # reference only...can do copy if needed (safer)

    def _bitset_to_tags(
        self,
        bitset: libression.entities.db.TagBitSet,
        cursor: sqlite3.Cursor,
    ) -> list[str]:
        """Convert TagBitSet back to list of tag names."""
        tag_ids = bitset.get_tag_ids()  # Get actual set bits, no range limitation
        tag_map = self._sync_tags_by_tag_ids(tag_ids, cursor)

        return [x for x in tag_map.get_names(tag_ids) if x is not None]

    def _files_in_db_to_entries(
        self,
        rows: list[sqlite3.Row],
        cursor: sqlite3.Cursor,
    ) -> list[libression.entities.db.DBFileEntry]:
        """Convert SQLite rows to DBFileEntry objects."""
        entries = []
        for row in rows:
            # Convert row to dict and handle special fields
            entry_dict = dict(row)
            entry_dict["table_id"] = entry_dict.pop("id")
            entry_dict["action_type"] = libression.entities.db.DBFileAction(
                entry_dict["action_type"]
            )

            # Convert tag_bits blob to TagBitSet if present
            if "tag_bits" in entry_dict:
                tag_bits = entry_dict.pop(
                    "tag_bits"
                )  # Remove from dict to avoid constructor error
                if tag_bits:
                    tags = self._bitset_to_tags(
                        libression.entities.db.TagBitSet.from_blob(tag_bits),
                        cursor,
                    )
                else:
                    tags = []
                entry_dict["tags"] = tags

            entries.append(libression.entities.db.DBFileEntry(**entry_dict))

        return entries

    def get_file_entries_by_file_keys(
        self,
        file_keys: list[str],
        chunk_size: int = 900,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Get current states of multiple files in chunks to avoid SQLite limits.
        Default chunk_size of 900 is conservative and safe for most SQLite builds.
        """
        if not file_keys:
            return []

        results = []
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            # Process in chunks to avoid SQLite variable limits
            for i in range(0, len(file_keys), chunk_size):
                chunk = file_keys[i : i + chunk_size]
                placeholders = ",".join(["?"] * len(chunk))

                chunk_results = cursor.execute(
                    f"""
                    WITH latest_states AS (
                        SELECT
                            file_key,
                            MAX(created_at) as latest_at
                        FROM file_actions
                        WHERE file_key IN ({placeholders})
                        GROUP BY file_key
                    )
                    SELECT
                        f.*,
                        ft.tag_bits
                    FROM file_actions f
                    JOIN latest_states ls ON f.file_key = ls.file_key
                        AND f.created_at = ls.latest_at
                    LEFT JOIN file_tags ft ON ft.file_id = f.id
                    ORDER BY f.created_at DESC, f.id DESC
                """,
                    chunk,
                ).fetchall()

                results.extend(chunk_results)

            return self._files_in_db_to_entries(results, cursor)

    def get_file_entries_by_tags(
        self,
        include_tag_names: typing.Sequence[str] = tuple(),
        exclude_tag_names: typing.Sequence[str] = tuple(),
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Find files matching tag criteria:
        - include_tag_names: files must have ALL these tags
        - exclude_tag_names: files must have NONE of these tags
        """

        if not include_tag_names and not exclude_tag_names:
            raise ValueError("At least one tag must be provided!")

        if len(include_tag_names) > len(set(include_tag_names)):
            raise ValueError("Include tags cannot have duplicates!")

        if len(exclude_tag_names) > len(set(exclude_tag_names)):
            raise ValueError("Exclude tags cannot have duplicates!")

        if set(include_tag_names).intersection(set(exclude_tag_names)):
            raise ValueError("Include and exclude tags cannot overlap!")

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            query = """
                WITH latest_states AS (
                    SELECT
                        file_key,
                        file_entity_uuid,
                        action_type,
                        MAX(created_at) as latest_at
                    FROM file_actions
                    GROUP BY file_key
                )
                SELECT
                    f.*,
                    ft.tag_bits
                FROM file_actions f
                JOIN latest_states ls ON f.file_key = ls.file_key
                    AND f.created_at = ls.latest_at
                JOIN file_tags ft ON ft.file_entity_uuid = f.file_entity_uuid
                WHERE ls.action_type != 'DELETE'
            """

            params = []
            if include_tag_names:
                mapping = self._sync_tags_by_tag_names(list(include_tag_names), cursor)
                tag_ids = [
                    x for x in mapping.get_ids(include_tag_names) if x is not None
                ]
                include_bits = libression.entities.db.TagBitSet.from_tag_ids(tag_ids)
                query += (
                    " AND (ft.tag_bits & ?) = ?"  # All required tags must be present
                )
                params.extend([include_bits.to_blob(), include_bits.to_blob()])

            if exclude_tag_names:
                mapping = self._sync_tags_by_tag_names(list(exclude_tag_names), cursor)
                tag_ids = [
                    x for x in mapping.get_ids(exclude_tag_names) if x is not None
                ]
                exclude_bits = libression.entities.db.TagBitSet.from_tag_ids(tag_ids)
                query += " AND (ft.tag_bits & ?) = 0"  # No excluded tags can be present
                params.append(exclude_bits.to_blob())

            query += " ORDER BY f.created_at DESC, f.id DESC"

            rows = cursor.execute(query, params).fetchall()
            return self._files_in_db_to_entries(rows, cursor)

    def get_file_history(
        self, file_key: str
    ) -> list[libression.entities.db.DBFileEntry]:
        """Get complete history of a file."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            # First get the entity_uuid from most recent state
            latest = cursor.execute(
                """
                SELECT file_entity_uuid
                FROM file_actions
                WHERE file_key = ?
                ORDER BY created_at DESC, id DESC  -- Order by both timestamp and id
                LIMIT 1
            """,
                (file_key,),
            ).fetchone()

            if not latest:
                return []

            # Then get all records for this entity
            rows = cursor.execute(
                """
                SELECT
                    f.*,
                    ft.tag_bits,
                    ft.created_at as tags_updated_at
                FROM file_actions f
                LEFT JOIN file_tags ft ON ft.file_entity_uuid = f.file_entity_uuid
                WHERE f.file_entity_uuid = ?
                ORDER BY f.created_at DESC, f.id DESC  -- Order by both timestamp and id
            """,
                (latest["file_entity_uuid"],),
            ).fetchall()

            return self._files_in_db_to_entries(rows, cursor)

    def get_file_history_by_id(
        self, file_id: int
    ) -> list[libression.entities.db.DBFileEntry]:
        """Get complete history of a file using its ID."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            # Get file_entity_uuid first
            file = cursor.execute(
                "SELECT file_entity_uuid FROM file_actions WHERE id = ?", (file_id,)
            ).fetchone()

            if not file:
                return []

            # Then get all records for this entity
            rows = cursor.execute(
                """
                SELECT
                    f.*,
                    ft.tag_bits,
                    ft.created_at as tags_updated_at
                FROM file_actions f
                LEFT JOIN file_tags ft ON ft.file_entity_uuid = f.file_entity_uuid
                WHERE f.file_entity_uuid = ?
                ORDER BY f.created_at DESC
            """,
                (file["file_entity_uuid"],),
            ).fetchall()

            return self._files_in_db_to_entries(rows, cursor)

    def find_similar_files(
        self, file_key: str
    ) -> list[libression.entities.db.DBFileEntry]:
        """Find similar files using both checksum and phash."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            rows = cursor.execute(
                """
                WITH target AS (
                    SELECT thumbnail_checksum, thumbnail_phash
                    FROM file_actions
                    WHERE file_key = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                ),
                latest_states AS (
                    SELECT
                        file_key,
                        action_type,
                        thumbnail_checksum,
                        thumbnail_phash,
                        MAX(created_at) as latest_at
                    FROM file_actions
                    GROUP BY file_key
                )
                SELECT f.*
                FROM file_actions f
                JOIN latest_states ls ON f.file_key = ls.file_key
                    AND f.created_at = ls.latest_at
                CROSS JOIN target t
                WHERE ls.action_type != 'DELETE'
                AND (
                    ls.thumbnail_checksum = t.thumbnail_checksum
                    OR ls.thumbnail_phash = t.thumbnail_phash
                )
                ORDER BY
                    CASE
                        WHEN ls.thumbnail_checksum = t.thumbnail_checksum
                        AND ls.thumbnail_phash = t.thumbnail_phash THEN 1
                        WHEN ls.thumbnail_checksum = t.thumbnail_checksum THEN 2
                        ELSE 3
                    END,
                    f.created_at DESC
            """,
                (file_key,),
            ).fetchall()

            return self._files_in_db_to_entries(rows, cursor)
