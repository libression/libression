"""Initial schema

Revision ID: 001
Revises: 
Create Date: 2024-12-24 10:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Core files table - insert only, includes operation info
    op.create_table('file_actions',
        sa.Column('id', sa.Integer(), primary_key=True),  # auto-generated (sqlite primary key set to integer auto-increments)
        sa.Column('file_entity_uuid', sa.String(36), nullable=False),  # UUID for tracking file history (file_actions + edits)
        sa.Column('file_key', sa.String(), nullable=False),  # path to file (webdav/s3)
        sa.Column('action_type', sa.String(), nullable=False),
        # Optional fields (thumbnails and phash can fail ... or be refreshed)
        sa.Column('thumbnail_key', sa.String(), nullable=True),  # path to thumbnail (webdav/s3)
        sa.Column('thumbnail_checksum', sa.String(64), nullable=True),  # SHA256
        sa.Column('thumbnail_phash', sa.String(), nullable=True),  # phash of file (4X4 rotational greyscale of frames)
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),  # auto-generated
    )
    
    # Tags reference table
    op.create_table('tags',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(), nullable=False, unique=True),
    )
    
    # File tags - insert only
    op.create_table('file_tags',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('file_id', sa.Integer(), nullable=False),
        sa.Column('tag_bits', sa.LargeBinary(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.ForeignKeyConstraint(['file_id'], ['file_actions.id'])
    )
    
    # Indexes
    op.create_index('idx_file_actions_key_time', 'file_actions', ['file_key', 'created_at'])
    op.create_index('idx_file_tags_file_time', 'file_tags', ['file_id', 'created_at'])
    op.create_index('idx_file_entity_uuid', 'file_actions', ['file_entity_uuid'])
    op.create_index('idx_files_phash', 'file_actions', ['thumbnail_phash'])
    op.create_index('idx_files_checksums', 'file_actions', ['thumbnail_checksum', 'thumbnail_phash'])

def downgrade():
    op.drop_table('file_tags')
    op.drop_table('tags')
    op.drop_table('file_actions')
