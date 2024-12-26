import pytest
import libression.entities.db
import libression.db.client

@pytest.fixture
def db_client(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    client = libression.db.client.DBClient(db_path)
    yield client
    # Clean up
    if db_path.exists():
        db_path.unlink()

@pytest.fixture
def sample_entries():
    """Create sample file entries for testing."""
    return [
        libression.entities.db.new_db_file_entry(
            file_key="test1.jpg",
            thumbnail_key="thumb1.jpg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg"
        )
    ]

def test_insert_files(db_client, sample_entries):
    """Test inserting new files."""
    ids = db_client.insert_to_files_table(sample_entries)
    assert len(ids) == 1
    
    # Verify entries
    state = db_client.get_file_state("test1.jpg")
    assert state is not None
    assert state['file_key'] == "test1.jpg"
    assert state['thumbnail_checksum'] == "abc123"
    assert state['action_type'] == "CREATE"
    assert state['mime_type'] == "image/jpeg"


def test_file_history(db_client, sample_entries):
    """Test file history tracking."""
    # Create initial file
    [file_id] = db_client.insert_to_files_table([sample_entries[0]])
    initial_state = db_client.get_file_state("test1.jpg")
    
    # Debug output
    print(f"\nInitial state: {dict(initial_state)}")
    
    # Move file
    moved = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        thumbnail_key="thumb1.jpg",
        thumbnail_checksum="abc123",
        thumbnail_phash="def456",
        action_type=libression.entities.db.DBFileAction.MOVE,
        file_entity_uuid=initial_state['file_entity_uuid']
    )
    db_client.insert_to_files_table([moved])
    
    # Debug: verify both records exist
    with db_client._connect() as conn:
        all_records = conn.execute(
            "SELECT * FROM file_actions ORDER BY created_at DESC"
        ).fetchall()
        print("\nAll records:")
        for record in all_records:
            print(dict(record))
    
    # Check history
    history = db_client.get_file_history("moved.jpg")
    print(f"\nHistory for moved.jpg: {[dict(h) for h in history]}")
    
    assert len(history) == 2
    assert history[0]['file_key'] == "moved.jpg"
    assert history[1]['file_key'] == "test1.jpg"

def test_tag_operations(db_client, sample_entries):
    """Test tag operations."""
    # Create files
    file_ids = db_client.insert_to_files_table(sample_entries)
    
    # Create tags
    tag_ids = db_client.ensure_tags(["tag1", "tag2", "tag3"])
    assert len(tag_ids) == 3
    print(f"\nCreated tag IDs: {tag_ids}")
    
    # Create tag bitset
    tag_bits = libression.entities.db.TagBitSet.new()
    tag_bits.add_tag(tag_ids[0])
    tag_bits.add_tag(tag_ids[2])
    
    # Debug tag bits
    print(f"\nTag bits blob: {tag_bits.to_blob().hex()}")
    print("Tag bits set:", [i for i in range(256) if tag_bits.has_tag(i)])
    
    # Add tags to files
    tag_record_ids = db_client.update_file_tags(file_ids, tag_bits.to_blob())
    
    # Debug: verify tags were added correctly
    with db_client._connect() as conn:
        print("\nFile tags in database:")
        for row in conn.execute("SELECT file_id, tag_bits FROM file_tags"):
            print(f"File ID: {row['file_id']}, Raw bits: {row['tag_bits'].hex()}")
            bits = libression.entities.db.TagBitSet.from_blob(row['tag_bits'])
            print(f"  Tag bits set: {[i for i in range(256) if bits.has_tag(i)]}")
    
    # Query files with tags
    files = db_client.get_files_with_tags(tag_bits.to_blob())
    print(f"\nFiles with tags: {[dict(f) for f in files]}")
    
    assert len(files) == 1
    assert files[0]['file_key'] == "test1.jpg"
    assert libression.entities.db.TagBitSet.from_blob(files[0]['tag_bits']).has_tag(tag_ids[0])

def test_similar_files(db_client, sample_entries):
    """Test finding similar files."""
    # Create files with same checksum but different phash
    similar = libression.entities.db.new_db_file_entry(
        file_key="similar.jpg",
        thumbnail_key="thumb3.jpg",
        thumbnail_checksum=sample_entries[0].thumbnail_checksum,  # Same checksum
        thumbnail_phash="different123"  # Different phash
    )
    
    db_client.insert_to_files_table(sample_entries + [similar])
    
    # Find similar files
    similar_files = db_client.find_similar_files("test1.jpg")
    assert len(similar_files) == 2  # Original + similar
    assert any(f['file_key'] == "similar.jpg" for f in similar_files)

def test_error_cases(db_client):
    """Test error handling."""
    # Test empty insert
    assert db_client.insert_to_files_table([]) == []
    
    # Test invalid action type
    with pytest.raises(ValueError):
        libression.entities.db.existing_db_file_entry(
            file_key="test.jpg",
            thumbnail_key="thumb.jpg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            action_type=libression.entities.db.DBFileAction.CREATE,  # Should use create() for CREATE actions
            file_entity_uuid="123"
        )
    
    # Test non-existent file
    assert db_client.get_file_state("nonexistent.jpg") is None
    assert db_client.get_file_history("nonexistent.jpg") == []
