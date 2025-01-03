import dataclasses
import datetime
import pathlib
import sqlite3
import typing
import contextlib
import alembic.command
from alembic.config import Config

import libression.entities.db


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

    @contextlib.contextmanager
    def _get_connection(self) -> typing.Generator[sqlite3.Connection, None, None]:
        connection = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        # Enable dictionary-like row access
        connection.row_factory = sqlite3.Row

        # Performance and durability settings
        connection.execute(
            "PRAGMA journal_mode=WAL"
        )  # Write-Ahead Logging for better concurrency
        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )  # Good balance of safety and speed
        connection.execute("PRAGMA datetime_precision=6")  # Microsecond precision
        connection.execute("PRAGMA foreign_keys=ON")  # Enforce foreign key constraints

        yield connection

        connection.commit()
        connection.close()

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
        tag_names: list[str],
        cursor: sqlite3.Cursor,
    ) -> libression.entities.db.TagMapping:
        """
        Ensure tags exist in the database
        lazy syncs (only calls db when missing tags)
        """

        offline_tag_mapping = self._cache_tag_mapping(cursor, force_update=False)

        missing_tags = set(tag_names) - set(offline_tag_mapping.name_to_id.keys())

        if not missing_tags:
            return offline_tag_mapping  # no need to call db

        cursor.executemany(
            # If clashes in name, safely ignore (as the tag is already registered)
            # could be that offline_tag_mapping is out of sync, but we don't care
            "INSERT OR IGNORE INTO tags (name) VALUES (?)",
            [(name,) for name in missing_tags],
        )

        return self._cache_tag_mapping(cursor, force_update=True)

    def _insert_file_tags(
        self,
        entries: list[libression.entities.db.DBFileEntry],
        cursor: sqlite3.Cursor,
    ) -> None:
        """
        requires a connected cursor
        """

        if not entries:
            return None  # nothing to do

        invalid_entries = [entry for entry in entries if not entry.file_entity_uuid]

        if invalid_entries:
            raise ValueError("Entries with no entity_uuid cannot be inserted!")

        # Filter entries with tags and prepare parameters
        tag_params = []

        tags_created_at = datetime.datetime.now(
            datetime.UTC
        )  # Inserts grouped by timestamp and file_entity_uuid

        for entry in entries:
            tag_mapping = self._sync_tags_by_tag_names(list(entry.tags), cursor)

            for tag_name in entry.tags:
                tag_id = tag_mapping.name_to_id[tag_name]
                tag_params.append((entry.file_entity_uuid, tag_id, tags_created_at))

        if tag_params:  # Only execute if we have tags to insert
            cursor.executemany(
                "INSERT INTO file_tags (file_entity_uuid, tag_id, tags_created_at) VALUES (?, ?, ?)",
                tag_params,
            )

    def register_file_tags(
        self,
        entries: list[libression.entities.db.DBFileEntry],
    ) -> None:
        """Register tags for files."""
        if not entries:
            return

        with self._get_connection() as conn:
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
        """Insert entries into file_actions table and return (id, action_created_at) pairs."""
        return [
            cursor.execute(
                """
                INSERT INTO file_actions (
                    file_entity_uuid,
                    file_key,
                    action_type,
                    thumbnail_key,
                    thumbnail_mime_type,
                    thumbnail_checksum,
                    thumbnail_phash,
                    mime_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id, action_created_at;
                """,
                (
                    entry.file_entity_uuid,
                    entry.file_key,
                    entry.action_type.value,
                    entry.thumbnail_key,
                    entry.thumbnail_mime_type,
                    entry.thumbnail_checksum,
                    entry.thumbnail_phash,
                    entry.mime_type,
                ),
            ).fetchone()
            for entry in entries
        ]

    def register_file_action(
        self,
        entries: list[libression.entities.db.DBFileEntry],
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Insert entries into file_actions tables (NOT file_tags)
        Returns the fully populated objects (with ids, timestamps, etc.).
        """
        if not entries:
            return []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            created_at_ids = self._insert_file_actions(entries, cursor)

            registered_entries = []
            for i, entry in enumerate(entries):
                entry_dict = entry._asdict()
                entry_dict["action_created_at"] = created_at_ids[i][
                    1
                ]  # Update instead of add
                registered_entries.append(
                    libression.entities.db.DBFileEntry.from_dict(entry_dict)
                )

            return registered_entries

    ############################################################################################
    # query methods
    ############################################################################################

    def _file_entry_from_db_row(
        self,
        row: sqlite3.Row,
        cursor: sqlite3.Cursor,
    ) -> libression.entities.db.DBFileEntry:
        """
        Parses a row from the file_actions table into a DBFileEntry object.

        Checks mandatory fields:
        - file_entity_uuid
        - file_key
        - action_type (to DBFileAction enum)
        - action_created_at

        Fields that are parsed unchecked:
        - mime_type
        - thumbnail_key
        - thumbnail_checksum
        - thumbnail_phash

        Tags are parsed from tag_ids (str(list[int])) to tag_names (list[str])
        Requires connected cursor (for lazy cache of tag_mapping)
        """
        row_dict = dict(row)

        mandatory_fields = [
            "file_entity_uuid",
            "file_key",
            "action_type",
            "action_created_at",
        ]

        if any([x not in row_dict for x in mandatory_fields]):
            raise ValueError("Missing required fields in row!")

        row_dict["action_type"] = libression.entities.db.DBFileAction(
            row_dict["action_type"]
        )

        # Handle tags
        if "tag_ids" in row_dict:
            tag_ids = row_dict.pop("tag_ids")

            if not tag_ids:  # empty string or None in tag_ids (no tags...)
                return libression.entities.db.DBFileEntry.from_dict(
                    row_dict
                )  # early bail

            itemised_tag_ids = tag_ids.split(",")

            tag_mapping = self._cache_tag_mapping(cursor, force_update=False)

            if set(int(tag_id) for tag_id in itemised_tag_ids) - set(
                tag_mapping.id_to_name.keys()
            ):
                tag_mapping = self._cache_tag_mapping(cursor, force_update=True)

            row_dict["tags"] = [
                tag_mapping.id_to_name[int(tag_id)] for tag_id in itemised_tag_ids
            ]

        return libression.entities.db.DBFileEntry.from_dict(row_dict)

    def get_file_entries_by_file_keys(
        self,
        file_keys: list[str],
        chunk_size: int = 900,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Get current states of multiple files in chunks to avoid SQLite limits.
        Default chunk_size of 900 is conservative and safe for most SQLite builds.
        combines latest file_actions and file_tags
        """
        if not file_keys:
            return []

        results = []
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            # Process in chunks to avoid SQLite variable limits
            for i in range(0, len(file_keys), chunk_size):
                chunk = file_keys[i : i + chunk_size]
                placeholders = ",".join(["?"] * len(chunk))

                # Base query for latest non-deleted state
                query = """
                WITH latest_states AS (
                    SELECT
                        file_key,
                        MAX(action_created_at) as latest_at
                    FROM file_actions
                    WHERE file_key IN ({})
                    AND action_type != 'DELETE'
                    GROUP BY file_key
                )
                , latest_tags AS (
                    SELECT
                        ft.file_entity_uuid,
                        GROUP_CONCAT(ft.tag_id) as tag_ids
                    FROM file_tags ft
                    JOIN (
                        SELECT file_entity_uuid, MAX(tags_created_at) as max_created
                        FROM file_tags
                        GROUP BY file_entity_uuid, tag_id
                    ) lt ON ft.file_entity_uuid = lt.file_entity_uuid
                        AND ft.tags_created_at = lt.max_created
                    GROUP BY ft.file_entity_uuid
                )
                SELECT f.*, COALESCE(lt.tag_ids, '') as tag_ids
                FROM file_actions f
                JOIN latest_states ls
                    ON f.file_key = ls.file_key
                    AND f.action_created_at = ls.latest_at
                LEFT JOIN latest_tags lt
                    ON lt.file_entity_uuid = f.file_entity_uuid
                ORDER BY f.action_created_at DESC, f.id DESC
                """.format(placeholders)

                rows = cursor.execute(query, chunk).fetchall()
                results.extend(
                    [self._file_entry_from_db_row(row, cursor) for row in rows]
                )

        return results

    def get_file_entries_by_tags(
        self,
        include_tag_groups: list[list[str]] = [],
        exclude_tags: list[str] = [],
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Find files matching tag criteria:
        - include_tag_groups: files must match ANY group of tags (OR between groups)
                             within each group, ALL tags must match (AND within group)
        - exclude_tags: files must not have ANY of these tags (OR between tags)

        Example:
            include_tag_groups=[["vacation", "beach"], ["work", "important"]]
            -> Files that have (vacation AND beach) OR (work AND important)

            exclude_tags=["private", "draft"]
            -> Files that don't have private OR draft
        """
        if not include_tag_groups and not exclude_tags:
            raise ValueError("At least one tag must be provided!")

        # Validate input
        for group in include_tag_groups:
            if len(group) > len(set(group)):
                raise ValueError("Tag groups cannot have duplicates!")

        if len(exclude_tags) > len(set(exclude_tags)):
            raise ValueError("Exclude tags cannot have duplicates!")

        # Check for overlaps between include and exclude
        all_include_tags = {tag for group in include_tag_groups for tag in group}
        if all_include_tags.intersection(exclude_tags):
            raise ValueError("Include and exclude tags cannot overlap!")

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Base query with latest actions and tags
            query = """
            WITH latest_actions AS (
                -- Get latest non-deleted action for each file
                SELECT
                    file_entity_uuid,
                    file_key,
                    MAX(action_created_at) as latest_action
                FROM file_actions
                WHERE action_type != 'DELETE'
                GROUP BY file_entity_uuid
            ),
            latest_tags AS (
                -- Get latest tags for each file
                SELECT
                    file_entity_uuid,
                    tag_id,
                    MAX(tags_created_at) as max_created
                FROM file_tags
                GROUP BY file_entity_uuid, tag_id
            )
            SELECT
                f.*,
                GROUP_CONCAT(lt.tag_id) as tag_ids
            FROM file_actions f
            JOIN latest_actions la
                ON f.file_entity_uuid = la.file_entity_uuid
                AND f.action_created_at = la.latest_action
            LEFT JOIN latest_tags lt
                ON lt.file_entity_uuid = f.file_entity_uuid
            """

            conditions = []
            params = []

            if include_tag_groups:
                include_conditions = []

                for group in include_tag_groups:
                    # Get tag IDs for this group
                    placeholders = ",".join("?" * len(group))
                    cursor.execute(
                        f"SELECT id FROM tags WHERE name IN ({placeholders})",
                        list(group),
                    )
                    group_ids = [row["id"] for row in cursor.fetchall()]

                    # Must have ALL tags in this group
                    include_conditions.append(f"""
                        f.file_entity_uuid IN (
                            SELECT file_entity_uuid
                            FROM latest_tags lt
                            WHERE lt.tag_id IN ({",".join("?" * len(group_ids))})
                            GROUP BY file_entity_uuid
                            HAVING COUNT(DISTINCT lt.tag_id) = {len(group_ids)}
                        )
                    """)
                    params.extend(group_ids)

                # OR between groups
                if include_conditions:
                    conditions.append("(" + " OR ".join(include_conditions) + ")")

            if exclude_tags:
                # Get tag IDs for exclude tags
                placeholders = ",".join("?" * len(exclude_tags))
                cursor.execute(
                    f"SELECT id FROM tags WHERE name IN ({placeholders})",
                    list(exclude_tags),
                )
                exclude_ids = [row["id"] for row in cursor.fetchall()]

                # Must not have ANY of these tags
                conditions.append(f"""
                    f.file_entity_uuid NOT IN (
                        SELECT file_entity_uuid
                        FROM latest_tags lt
                        WHERE lt.tag_id IN ({",".join("?" * len(exclude_ids))})
                    )
                """)
                params.extend(exclude_ids)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " GROUP BY f.id"  # Add GROUP BY for tag_ids concatenation

            rows = cursor.execute(query, params).fetchall()
            return [self._file_entry_from_db_row(row, cursor) for row in rows]

    def get_file_history(
        self, file_key: str
    ) -> list[libression.entities.db.DBFileEntry]:
        """Get history of file actions (CREATE/UPDATE/MOVE/DELETE)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            # First get the entity_uuid from most recent state
            latest = cursor.execute(
                """
                SELECT file_entity_uuid
                FROM file_actions
                WHERE file_key = ?
                ORDER BY action_created_at DESC, id DESC
                LIMIT 1
                """,
                (file_key,),
            ).fetchone()

            if not latest:
                return []

            # Get all actions for this file entity
            rows = cursor.execute(
                """
                SELECT
                    f.*,
                    (
                        SELECT GROUP_CONCAT(tag_id)
                        FROM file_tags
                        WHERE file_entity_uuid = f.file_entity_uuid
                        AND tags_created_at <= f.action_created_at
                    ) as tag_ids
                FROM file_actions f
                WHERE f.file_entity_uuid = ?
                ORDER BY f.action_created_at DESC, f.id DESC
                """,
                (latest["file_entity_uuid"],),
            ).fetchall()

            return [self._file_entry_from_db_row(row, cursor) for row in rows]

    def get_tag_history(
        self, file_key: str
    ) -> list[tuple[datetime.datetime, set[str]]]:
        """Get history of tag changes for a file."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # First get the entity_uuid
            latest = cursor.execute(
                """
                SELECT file_entity_uuid
                FROM file_actions
                WHERE file_key = ?
                ORDER BY action_created_at DESC, id DESC
                LIMIT 1
                """,
                (file_key,),
            ).fetchone()

            if not latest:
                return []

            # Get distinct tag states
            rows = cursor.execute(
                """
                WITH tag_states AS (
                    SELECT DISTINCT
                        ft.tags_created_at,
                        GROUP_CONCAT(t.name) as tag_names
                    FROM file_tags ft
                    JOIN tags t ON t.id = ft.tag_id
                    WHERE ft.file_entity_uuid = ?
                    GROUP BY ft.tags_created_at
                )
                SELECT *
                FROM tag_states
                ORDER BY tags_created_at DESC
                """,
                (latest["file_entity_uuid"],),
            ).fetchall()

            return [
                (row["tags_created_at"], set(row["tag_names"].split(",")))
                for row in rows
            ]

    def find_similar_files(
        self, file_key: str
    ) -> list[libression.entities.db.DBFileEntry]:
        """Find similar files using both checksum and phash."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")

            rows = cursor.execute(
                """
                WITH target AS (
                    SELECT thumbnail_checksum, thumbnail_phash
                    FROM file_actions
                    WHERE file_key = ?
                    ORDER BY action_created_at DESC
                    LIMIT 1
                ),
                latest_states AS (
                    SELECT
                        file_key,
                        action_type,
                        thumbnail_checksum,
                        thumbnail_phash,
                        MAX(action_created_at) as latest_at
                    FROM file_actions
                    GROUP BY file_key
                )
                SELECT f.*
                FROM file_actions f
                JOIN latest_states ls ON f.file_key = ls.file_key
                    AND f.action_created_at = ls.latest_at
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
                    f.action_created_at DESC
            """,
                (file_key,),
            ).fetchall()

            return [self._file_entry_from_db_row(row, cursor) for row in rows]
