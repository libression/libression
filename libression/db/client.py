import sqlite3
import pathlib
from alembic.config import Config
import alembic.command
import libression


class DBClient:
    def __init__(self, db_path: str | pathlib.Path):
        self.db_path = pathlib.Path(db_path)
        self._ensure_db()

    def _ensure_db(self) -> None:
        """Create database and apply migrations."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Setup Alembic config
        alembic_cfg = Config()
        alembic_cfg.set_main_option(
            'script_location', 
            str(pathlib.Path(__file__).parent / 'migrations'),
        )
        alembic_cfg.set_main_option(
            'sqlalchemy.url', 
            f'sqlite:///{self.db_path}',
        )
        
        # Run migrations
        alembic.command.upgrade(alembic_cfg, "head")

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection with proper settings."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Performance optimizations
        conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging
        conn.execute('PRAGMA synchronous=NORMAL')  # Faster writes, still safe
        
        # Use microsecond precision for timestamps
        conn.execute('PRAGMA datetime_precision=6')
        
        return conn

    # File operations
    def insert_to_files_table(self, entries: list[libression.entities.db.DBFileEntry]) -> list[int]:
        """
        Insert entries into files table.
        Returns list of new file IDs.
        """
        if not entries:
            return []

        if any(
            (entry.table_id is not None or entry.created_at is not None)
            for entry in entries
        ):
            raise ValueError("table_id and created_at must be None for insert!")

        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Get immediate write lock
            cursor.execute("BEGIN IMMEDIATE")
            
            ids = []
            for entry in entries:
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
                    RETURNING id
                    """,
                    (
                        entry.file_entity_uuid,
                        entry.file_key,
                        entry.action_type.value,
                        entry.thumbnail_key,
                        entry.thumbnail_checksum,
                        entry.thumbnail_phash,
                        entry.mime_type,
                    )
                )
                ids.append(cursor.fetchone()[0])
            return ids

    # Tag operations
    def ensure_tags(self, tag_names: list[str]) -> list[int]:
        """Bulk ensure tags exist. Returns tag IDs."""
        if not tag_names:
            return []
        
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            
            # Try to get existing tags
            placeholders = ','.join(['?'] * len(tag_names))
            existing = {
                row['name']: row['id']
                for row in cursor.execute(
                    f"SELECT id, name FROM tags WHERE name IN ({placeholders})",
                    tag_names
                )
            }

            # Insert missing tags one by one (since we need RETURNING)
            for name in tag_names:
                if name not in existing:
                    cursor.execute(
                        "INSERT INTO tags (name) VALUES (?) RETURNING id",
                        (name,)
                    )
                    existing[name] = cursor.fetchone()[0]

            # Return IDs in original order
            return [existing[name] for name in tag_names]

    def update_file_tags(self, file_ids: list[int], tag_bits: bytes) -> list[int]:
        """Bulk update file tags. Returns file_tag IDs."""
        with self._connect() as conn:
            # Insert new tag records with RETURNING
            return [
                row[0] for row in conn.executemany(
                    """
                    INSERT INTO file_tags (file_id, tag_bits)
                    VALUES (?, ?)
                    RETURNING id
                    """,
                    [(file_id, tag_bits) for file_id in file_ids]
                )
            ]
    # Query operations
    def get_file_state(self, file_key: str) -> dict | None:
        """Get current state of a file."""
        with self._connect() as conn:
            return conn.execute("""
                SELECT 
                    f.*,
                    ft.tag_bits
                FROM file_actions f
                LEFT JOIN file_tags ft ON ft.file_id = f.id
                WHERE f.file_key = ?
                ORDER BY f.created_at DESC
                LIMIT 1
            """, (file_key,)).fetchone()

    def get_files_with_tags(self, tag_bits: bytes) -> list[dict]:
        """Find all files with specific tags."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            
            # Debug the values
            print(f"\nSearching for tag bits: {tag_bits.hex()}")
            
            # First just get all files with tags to see what we have
            all_tagged = cursor.execute("""
                SELECT 
                    f.file_key,
                    ft.tag_bits,
                    hex(ft.tag_bits) as hex_bits
                FROM file_actions f
                JOIN file_tags ft ON ft.file_id = f.id
            """).fetchall()
            
            print("\nAll tagged files:")
            for row in all_tagged:
                print(f"File: {row['file_key']}, Tags: {row['hex_bits']}")
            
            # Now try the actual query with hex comparison
            return cursor.execute("""
                WITH latest_states AS (
                    SELECT 
                        file_key,
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
                JOIN file_tags ft ON ft.file_id = f.id
                WHERE ls.action_type != 'DELETE'
                AND hex(ft.tag_bits) = hex(?)  -- Compare hex strings
                ORDER BY f.created_at DESC, f.id DESC
            """, (tag_bits,)).fetchall()

    def get_file_history(self, file_key: str) -> list[dict]:
        """Get complete history of a file."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            
            # First get the entity_uuid from most recent state
            latest = cursor.execute("""
                SELECT file_entity_uuid
                FROM file_actions
                WHERE file_key = ?
                ORDER BY created_at DESC, id DESC  -- Order by both timestamp and id
                LIMIT 1
            """, (file_key,)).fetchone()
            
            if not latest:
                return []
            
            # Then get all records for this entity
            return cursor.execute("""
                SELECT 
                    f.*,
                    ft.tag_bits,
                    ft.created_at as tags_updated_at
                FROM file_actions f
                LEFT JOIN file_tags ft ON ft.file_id = f.id
                WHERE f.file_entity_uuid = ?
                ORDER BY f.created_at DESC, f.id DESC  -- Order by both timestamp and id
            """, (latest['file_entity_uuid'],)).fetchall()

    def get_file_history_by_id(self, file_id: int) -> list[dict]:
        """Get complete history of a file using its ID."""
        with self._connect() as conn:
            # Get file_entity_uuid first
            file = conn.execute(
                "SELECT file_entity_uuid FROM file_actions WHERE id = ?",
                (file_id,)
            ).fetchone()
            
            if not file:
                return []
            
            # Then get all records for this entity
            return conn.execute("""
                SELECT 
                    f.*,
                    ft.tag_bits,
                    ft.created_at as tags_updated_at
                FROM file_actions f
                LEFT JOIN file_tags ft ON ft.file_id = f.id
                WHERE f.file_entity_uuid = ?
                ORDER BY f.created_at DESC
            """, (file['file_entity_uuid'],)).fetchall()

    def find_similar_files(self, file_key: str) -> list[dict]:
        """Find similar files using both checksum and phash."""
        with self._connect() as conn:
            return conn.execute("""
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
            """, (file_key,)).fetchall()
