import pytest

import libression.entities.db


@pytest.fixture
def sample_entries(dummy_file_key):
    """Create sample file entries for testing."""
    return [
        libression.entities.db.new_db_file_entry(
            file_key=dummy_file_key,
            thumbnail_key="thumb1.jpg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=["tag1", "tag2"],
        )
    ]


def test_register_file_action(db_client, sample_entries, dummy_file_key):
    """Test registering new files with tags."""
    registered = db_client.register_file_action(sample_entries)
    assert len(registered) == 1

    # Verify entries
    states = db_client.get_file_entries_by_file_keys([dummy_file_key])
    assert len(states) == 1
    state = states[0]
    assert state.file_key == dummy_file_key
    assert state.thumbnail_checksum == "abc123"
    assert state.action_type == libression.entities.db.DBFileAction.CREATE
    assert state.mime_type == "image/jpeg"
    assert len(state.tags) == 0  # No tags


def test_file_history(db_client, sample_entries, dummy_file_key):
    """Test file history tracking."""
    # Create initial file
    [initial_state] = db_client.register_file_action(sample_entries)

    # Verify initial history
    initial_history = db_client.get_file_history(dummy_file_key)
    assert len(initial_history) == 1, (
        f"Initial history should have exactly 1 entry, got {len(initial_history)}. "
        f"Entries: {[(h.file_key, h.action_type) for h in initial_history]}"
    )
    assert initial_history[0].file_key == dummy_file_key
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
    db_client.register_file_action([moved])[0]

    # Verify moved file history
    moved_history = db_client.get_file_history("moved.jpg")
    assert len(moved_history) == 2, (
        f"Move history should have exactly 2 entries, got {len(moved_history)}. "
        f"Entries: {[(h.file_key, h.action_type) for h in moved_history]}"
    )

    # Verify history order (most recent first)
    assert moved_history[0].file_key == "moved.jpg"
    assert moved_history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert moved_history[1].file_key == dummy_file_key
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
    original_history = db_client.get_file_history(dummy_file_key)
    assert (
        len(original_history) == 2
    ), "Original file should show complete entity history (CREATE and MOVE)"
    assert original_history[0].file_key == "moved.jpg"
    assert original_history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert original_history[1].file_key == dummy_file_key
    assert original_history[1].action_type == libression.entities.db.DBFileAction.CREATE


def test_basic_tag_queries(db_client):
    """Test basic tag inclusion/exclusion."""
    entries = [
        libression.entities.db.new_db_file_entry(
            file_key="beach1.jpg", tags=["vacation", "beach", "summer"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="work1.jpg", tags=["work", "important"]
        ),
    ]
    registered = db_client.register_file_action(entries)
    db_client.register_file_tags(registered)

    beach_vacation = db_client.get_file_entries_by_tags(
        include_tag_groups=[["vacation", "beach"]]
    )
    assert len(beach_vacation) == 1
    assert beach_vacation[0].file_key == "beach1.jpg"


def test_complex_tag_queries(db_client):
    """Test complex tag group combinations."""
    entries = [
        libression.entities.db.new_db_file_entry(
            file_key="beach1.jpg", tags=["vacation", "beach", "summer"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="mountain1.jpg", tags=["vacation", "mountain", "winter"]
        ),
    ]
    registered = db_client.register_file_action(entries)
    db_client.register_file_tags(registered)

    results = db_client.get_file_entries_by_tags(
        include_tag_groups=[["summer", "beach"], ["winter", "mountain"]],
        exclude_tags=["work"],
    )
    assert len(results) == 2
    assert {f.file_key for f in results} == {"beach1.jpg", "mountain1.jpg"}


def test_tag_history(db_client):
    """Test tag history tracking."""
    entries = [
        libression.entities.db.new_db_file_entry(
            file_key="test.jpg", tags=["initial", "tags"]
        ),
    ]
    registered = db_client.register_file_action(entries)
    db_client.register_file_tags(registered)

    updated = libression.entities.db.existing_db_file_entry(
        file_key="test.jpg",
        file_entity_uuid=registered[0].file_entity_uuid,
        action_type=libression.entities.db.DBFileAction.UPDATE,
        tags=["updated", "tags"],
    )
    db_client.register_file_action([updated])
    db_client.register_file_tags([updated])

    history = db_client.get_tag_history("test.jpg")
    assert len(history) == 2
    assert history[0][1] == {"updated", "tags"}
    assert history[1][1] == {"initial", "tags"}


def test_tag_operations(db_client):
    """Test tag-based file queries."""
    # Create files with different tags
    entries = [
        libression.entities.db.new_db_file_entry(
            file_key="beach1.jpg", tags=["vacation", "beach", "summer"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="mountain1.jpg", tags=["vacation", "mountain", "winter"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="work1.jpg", tags=["work", "important"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="private1.jpg", tags=["work", "private"]
        ),
        libression.entities.db.new_db_file_entry(
            file_key="draft1.jpg", tags=["work", "draft", "important"]
        ),
    ]
    registered = db_client.register_file_action(entries)
    db_client.register_file_tags(registered)

    # Test single group include
    beach_vacation = db_client.get_file_entries_by_tags(
        include_tag_groups=[["vacation", "beach"]]
    )
    assert len(beach_vacation) == 1
    assert beach_vacation[0].file_key == "beach1.jpg"
    assert set(beach_vacation[0].tags) == {"vacation", "beach", "summer"}

    # Test multiple groups (OR)
    vacation_or_important = db_client.get_file_entries_by_tags(
        include_tag_groups=[["vacation"], ["important"]]
    )
    assert len(vacation_or_important) == 4  # beach1, mountain1, work1, draft1
    assert {f.file_key for f in vacation_or_important} == {
        "beach1.jpg",
        "mountain1.jpg",
        "work1.jpg",
        "draft1.jpg",
    }

    # Test complex groups
    summer_beach_or_winter_mountain = db_client.get_file_entries_by_tags(
        include_tag_groups=[["summer", "beach"], ["winter", "mountain"]]
    )
    assert len(summer_beach_or_winter_mountain) == 2
    assert {f.file_key for f in summer_beach_or_winter_mountain} == {
        "beach1.jpg",
        "mountain1.jpg",
    }

    # Test exclude
    non_private = db_client.get_file_entries_by_tags(
        include_tag_groups=[["work"]], exclude_tags=["private"]
    )
    assert len(non_private) == 2
    assert {f.file_key for f in non_private} == {"work1.jpg", "draft1.jpg"}

    # Test multiple excludes
    clean_work = db_client.get_file_entries_by_tags(
        include_tag_groups=[["work"]], exclude_tags=["private", "draft"]
    )
    assert len(clean_work) == 1
    assert clean_work[0].file_key == "work1.jpg"

    # Test complex query
    vacation_spots_no_winter = db_client.get_file_entries_by_tags(
        include_tag_groups=[["vacation", "beach"], ["vacation", "mountain"]],
        exclude_tags=["winter"],
    )
    assert len(vacation_spots_no_winter) == 1
    assert vacation_spots_no_winter[0].file_key == "beach1.jpg"

    # Test file history
    original_uuid = registered[0].file_entity_uuid
    moved = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        file_entity_uuid=original_uuid,
        action_type=libression.entities.db.DBFileAction.MOVE,
        tags=[],  # Tags handled separately
    )
    db_client.register_file_action([moved])

    # Check file history shows actions
    history = db_client.get_file_history("moved.jpg")
    assert len(history) == 2
    assert history[0].file_entity_uuid == original_uuid
    assert history[0].file_key == "moved.jpg"
    assert history[0].action_type == libression.entities.db.DBFileAction.MOVE
    assert history[1].file_entity_uuid == original_uuid
    assert history[1].file_key == "beach1.jpg"
    assert history[1].action_type == libression.entities.db.DBFileAction.CREATE

    # Test tag history
    tag_history = db_client.get_tag_history("moved.jpg")
    assert len(tag_history) == 1  # Initial tag registration
    assert tag_history[0][1] == {"vacation", "beach", "summer"}

    # Test tag updates
    updated = libression.entities.db.existing_db_file_entry(
        file_key="moved.jpg",
        file_entity_uuid=original_uuid,
        action_type=libression.entities.db.DBFileAction.UPDATE,
        tags={"vacation", "beach", "sunset"},
    )
    db_client.register_file_action([updated])
    db_client.register_file_tags([updated])

    # Verify tag history shows changes
    tag_history = db_client.get_tag_history("moved.jpg")
    assert len(tag_history) == 2
    assert tag_history[0][1] == {"vacation", "beach", "sunset"}  # Latest tags
    assert tag_history[1][1] == {"vacation", "beach", "summer"}  # Original tags


def test_similar_files(db_client, sample_entries, dummy_file_key):
    """Test finding similar files."""
    # Create files with same checksum but different phash
    similar = libression.entities.db.new_db_file_entry(
        file_key="similar.jpg",
        thumbnail_key="thumb3.jpg",
        thumbnail_checksum=sample_entries[0].thumbnail_checksum,  # Same checksum
        thumbnail_phash="different123",  # Different phash
    )

    db_client.register_file_action(sample_entries + [similar])

    # Find similar files
    similar_files = db_client.find_similar_files(dummy_file_key)
    assert len(similar_files) == 2  # Original + similar
    assert any(f.file_key == "similar.jpg" for f in similar_files)


def test_error_cases(db_client):
    """Test error handling."""
    # Test empty register
    assert db_client.register_file_action([]) == []

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


def test_tag_error_cases(db_client):
    """Test tag-related error cases."""
    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags()  # No tags

    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags(
            include_tag_groups=[["tag1", "tag1"]]  # Duplicate tags
        )

    with pytest.raises(ValueError):
        db_client.get_file_entries_by_tags(
            include_tag_groups=[["tag1"]],
            exclude_tags=["tag1"],  # Overlapping include/exclude
        )
