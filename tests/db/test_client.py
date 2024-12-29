import pytest

import libression.db.client
import libression.entities.db


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
            mime_type="image/jpeg",
            tags=["tag1", "tag2"],
        )
    ]


def test_register_files(db_client, sample_entries):
    """Test registering new files with tags."""
    registered = db_client.register_files(sample_entries)
    assert len(registered) == 1

    # Verify entries
    states = db_client.get_file_entries_by_file_keys(["test1.jpg"])
    assert len(states) == 1
    state = states[0]
    assert state.file_key == "test1.jpg"
    assert state.thumbnail_checksum == "abc123"
    assert state.action_type == libression.entities.db.DBFileAction.CREATE
    assert state.mime_type == "image/jpeg"
    assert set(state.tags) == {"tag1", "tag2"}


def test_file_history(db_client, sample_entries):
    """Test file history tracking."""
    # Create initial file
    [initial_state] = db_client.register_files(sample_entries)

    # Move file
    moved = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        thumbnail_key="thumb1.jpg",
        thumbnail_checksum="abc123",
        thumbnail_phash="def456",
        action_type=libression.entities.db.DBFileAction.MOVE,
        file_entity_uuid=initial_state.file_entity_uuid,
        tags=["tag1", "tag2"],  # Maintain tags
    )
    db_client.register_files([moved])

    # Check history
    history = db_client.get_file_history("moved.jpg")
    assert len(history) == 2
    assert history[0].file_key == "moved.jpg"
    assert history[1].file_key == "test1.jpg"
    assert set(history[0].tags) == {"tag1", "tag2"}


def test_tag_operations(db_client):
    """Test tag-based file queries."""
    # Create files with different tags
    entries = [
        libression.entities.db.new_db_file_entry(
            file_key="test1.jpg", tags=["vacation", "beach"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="test2.jpg", tags=["vacation", "mountain"]
        ),
        libression.entities.db.new_db_file_entry(file_key="test3.jpg", tags=["work"]),
    ]
    db_client.register_files(entries)

    # Test tag queries
    vacation_files = db_client.get_file_entries_by_tags(include_tag_names=["vacation"])
    assert len(vacation_files) == 2
    assert {f.file_key for f in vacation_files} == {"test1.jpg", "test2.jpg"}

    beach_files = db_client.get_file_entries_by_tags(
        include_tag_names=["vacation", "beach"]
    )
    assert len(beach_files) == 1
    assert beach_files[0].file_key == "test1.jpg"

    non_work_files = db_client.get_file_entries_by_tags(exclude_tag_names=["work"])
    assert len(non_work_files) == 2
    assert {f.file_key for f in non_work_files} == {"test1.jpg", "test2.jpg"}


def test_similar_files(db_client, sample_entries):
    """Test finding similar files."""
    # Create files with same checksum but different phash
    similar = libression.entities.db.new_db_file_entry(
        file_key="similar.jpg",
        thumbnail_key="thumb3.jpg",
        thumbnail_checksum=sample_entries[0].thumbnail_checksum,  # Same checksum
        thumbnail_phash="different123",  # Different phash
    )

    db_client.register_files(sample_entries + [similar])

    # Find similar files
    similar_files = db_client.find_similar_files("test1.jpg")
    assert len(similar_files) == 2  # Original + similar
    assert any(f.file_key == "similar.jpg" for f in similar_files)


def test_error_cases(db_client):
    """Test error handling."""
    # Test empty register
    assert db_client.register_files([]) == []

    # Test invalid action type
    with pytest.raises(ValueError):
        libression.entities.db.existing_db_file_entry(
            file_key="test.jpg",
            action_type=libression.entities.db.DBFileAction.CREATE,  # Should use new_db_file_entry for CREATE
            file_entity_uuid="123",
        )

    # Test non-existent file
    assert db_client.get_file_entries_by_file_keys(["nonexistent.jpg"]) == []
    assert db_client.get_file_history("nonexistent.jpg") == []

    # Test invalid tag queries
    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags()  # No tags provided

    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags(
            include_tag_names=["tag1", "tag1"]  # Duplicate tags
        )

    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags(
            include_tag_names=["tag1"],
            exclude_tag_names=["tag1"],  # Overlapping include/exclude
        )
