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
    assert len(state.tags) == 0  # No tags


def test_file_history(db_client, sample_entries):
    """Test file history tracking."""
    # Create initial file
    [initial_state] = db_client.register_files(sample_entries)

    # Verify initial history
    initial_history = db_client.get_file_history("test1.jpg")
    assert len(initial_history) == 1, (
        f"Initial history should have exactly 1 entry, got {len(initial_history)}. "
        f"Entries: {[(h.file_key, h.action_type) for h in initial_history]}"
    )
    assert initial_history[0].file_key == "test1.jpg"
    assert initial_history[0].action_type == libression.entities.db.DBFileAction.CREATE
    assert initial_history[0].file_entity_uuid == initial_state.file_entity_uuid

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
    db_client.register_files([moved])[0]

    # Verify moved file history
    moved_history = db_client.get_file_history("moved.jpg")
    assert len(moved_history) == 2, (
        f"Move history should have exactly 2 entries, got {len(moved_history)}. "
        f"Entries: {[(h.file_key, h.action_type) for h in moved_history]}"
    )

    # Verify history order (most recent first)
    assert moved_history[0].file_key == "moved.jpg"
    assert moved_history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert moved_history[1].file_key == "test1.jpg"
    assert moved_history[1].action_type == libression.entities.db.DBFileAction.CREATE

    # Verify file entity consistency
    assert (
        len({h.file_entity_uuid for h in moved_history}) == 1
    ), "All history entries should share the same file_entity_uuid"
    assert moved_history[0].file_entity_uuid == initial_state.file_entity_uuid

    # Verify no duplicate entries
    file_action_pairs = [(h.file_key, h.action_type) for h in moved_history]
    assert len(file_action_pairs) == len(
        set(file_action_pairs)
    ), "Found duplicate entries in history"

    # Verify original file history shows the complete entity history
    original_history = db_client.get_file_history("test1.jpg")
    assert (
        len(original_history) == 2
    ), "Original file should show complete entity history (CREATE and MOVE)"
    assert original_history[0].file_key == "moved.jpg"
    assert original_history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert original_history[1].file_key == "test1.jpg"
    assert original_history[1].action_type == libression.entities.db.DBFileAction.CREATE


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
    registered = db_client.register_files(entries)
    db_client.register_file_tags(registered)  # Register initial tags

    # Test tag queries
    vacation_files = db_client.get_file_entries_by_tags(include_tag_names=["vacation"])
    assert len(vacation_files) == 2
    assert {f.file_key for f in vacation_files} == {"test1.jpg", "test2.jpg"}
    # Verify tags are preserved
    assert set(vacation_files[0].tags) == {"vacation", "beach"}
    assert set(vacation_files[1].tags) == {"vacation", "mountain"}

    # Test multiple tag query
    beach_vacation = db_client.get_file_entries_by_tags(
        include_tag_names=["vacation", "beach"]
    )
    assert len(beach_vacation) == 1
    assert beach_vacation[0].file_key == "test1.jpg"
    assert set(beach_vacation[0].tags) == {"vacation", "beach"}

    # Test exclusion
    non_work = db_client.get_file_entries_by_tags(exclude_tag_names=["work"])
    assert len(non_work) == 2
    assert {f.file_key for f in non_work} == {"test1.jpg", "test2.jpg"}

    # Test file history (should show actions without tags)
    original_uuid = registered[0].file_entity_uuid
    moved = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        file_entity_uuid=original_uuid,
        action_type=libression.entities.db.DBFileAction.MOVE,
        tags=[],  # Tags handled separately
    )
    db_client.register_files([moved])

    # Check file history shows actions
    history = db_client.get_file_history("moved.jpg")
    assert len(history) == 2
    assert history[0].file_entity_uuid == original_uuid
    assert history[0].file_key == "moved.jpg"
    assert history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert history[1].file_entity_uuid == original_uuid
    assert history[1].file_key == "test1.jpg"
    assert history[1].action_type == libression.entities.db.DBFileAction.CREATE

    # Test tag history
    tag_history = db_client.get_tag_history("moved.jpg")
    assert len(tag_history) == 1  # Initial tag registration
    assert tag_history[0][1] == {"vacation", "beach"}

    # Test tag updates
    updated = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        file_entity_uuid=original_uuid,
        action_type=libression.entities.db.DBFileAction.UPDATE,
        tags={"vacation", "beach", "sunset"},
    )
    db_client.register_files([updated])
    db_client.register_file_tags([updated])

    # Verify tag history shows changes
    tag_history = db_client.get_tag_history("moved.jpg")
    assert len(tag_history) == 2
    assert tag_history[0][1] == {"vacation", "beach", "sunset"}  # Latest tags
    assert tag_history[1][1] == {"vacation", "beach"}  # Original tags


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
