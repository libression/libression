"""Initial schema

Revision ID: 001
Revises:
Create Date: 2024-12-24 10:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Core files table - insert only, includes operation info
    op.create_table(
        "file_actions",
        sa.Column(
            "id", sa.Integer(), primary_key=True
        ),  # auto-generated (sqlite primary key set to integer auto-increments)
        sa.Column(
            "file_entity_uuid", sa.String(36), nullable=False
        ),  # UUID for tracking file history (file_actions + edits)
        sa.Column("file_key", sa.String(), nullable=False),  # path to file (webdav/s3)
        sa.Column("action_type", sa.String(), nullable=False),
        sa.Column("mime_type", sa.String(), nullable=True),  # Adding MIME type column
        # Optional fields (thumbnails and phash can fail ... or be refreshed)
        sa.Column(
            "thumbnail_key", sa.String(), nullable=True
        ),  # path to thumbnail (webdav/s3)
        sa.Column("thumbnail_checksum", sa.String(64), nullable=True),  # SHA256
        sa.Column(
            "thumbnail_phash", sa.String(), nullable=True
        ),  # phash of file (4X4 rotational greyscale of frames)
        sa.Column(
            "action_created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),  # auto-generated
    )

    # Tags reference table
    op.create_table(
        "tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False, unique=True),
    )

    # File tags - insert only
    op.create_table(
        "file_tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_entity_uuid", sa.String(36), nullable=False),
        sa.Column("tag_id", sa.Integer(), nullable=False),
        # Force timestamp declaration ("collections" of latest tags must have same timestamp)
        sa.Column("tags_created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["file_entity_uuid"], ["file_actions.file_entity_uuid"]
        ),
        sa.ForeignKeyConstraint(["tag_id"], ["tags.id"]),
    )

    # File Actions indexes
    op.create_index(
        "idx_file_actions_key_time", "file_actions", ["file_key", "action_created_at"]
    )
    op.create_index("idx_file_entity_uuid", "file_actions", ["file_entity_uuid"])
    op.create_index("idx_files_phash", "file_actions", ["thumbnail_phash"])
    op.create_index(
        "idx_files_checksums", "file_actions", ["thumbnail_checksum", "thumbnail_phash"]
    )

    # Tags index
    op.create_index("idx_tags_name", "tags", ["name"])

    # File Tags index (compound covers all cases)
    op.create_index(
        "idx_file_tags_compound",
        "file_tags",
        ["file_entity_uuid", "tag_id", "tags_created_at"],
    )


def downgrade():
    op.drop_table("file_tags")
    op.drop_table("tags")
    op.drop_table("file_actions")
