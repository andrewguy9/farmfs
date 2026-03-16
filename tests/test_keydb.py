from io import BytesIO

import pytest
from farmfs.blobstore import FileBlobstore
from farmfs.keydb import BlobKeyDB, JsonKeyDB
from farmfs.keydb import KeyDBLike
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.fs import Path
from farmfs.fs import ensure_absent
from farmfs.snapshot import KeySnapshot
from farmfs.volume import validate_snapshot
from typing import Any


@pytest.fixture()
def tmp_Path(tmp_path):
    return Path(str(tmp_path))


class KeyDBWrapper:
    def __init__(self, datadir):
        self.root = Path(datadir)
        self.keydir = self.root.join("keys")
        self.tmpdir = self.root.join("tmp")
        self.blobdir = self.root.join("blobs")

    def __enter__(self):
        ensure_absent(self.root)
        self.root.mkdir()
        self.keydir.mkdir()
        self.tmpdir.mkdir()
        self.blobdir.mkdir()
        bs = FileBlobstore(self.blobdir, self.tmpdir)
        blob_db = BlobKeyDB(self.keydir, self.tmpdir, bs)
        return JsonKeyDB(blob_db)

    def __exit__(self, type, value, traceback):
        ensure_absent(self.root)


class BlobKeyDBWrapper:
    """Wrapper that exposes raw BlobKeyDB for lower-level tests."""
    def __init__(self, datadir):
        self.root = Path(datadir)
        self.keydir = self.root.join("keys")
        self.tmpdir = self.root.join("tmp")
        self.blobdir = self.root.join("blobs")

    def __enter__(self):
        ensure_absent(self.root)
        self.root.mkdir()
        self.keydir.mkdir()
        self.tmpdir.mkdir()
        self.blobdir.mkdir()
        self.bs = FileBlobstore(self.blobdir, self.tmpdir)
        return BlobKeyDB(self.keydir, self.tmpdir, self.bs)

    def __exit__(self, type, value, traceback):
        ensure_absent(self.root)


def keydb_generic_test(db: KeyDBLike, expected_value: Any) -> None:
    assert db.list() == []
    db.write("five", 5, False)
    assert db.list() == ["five"]
    value = db.read("five")
    assert value == expected_value
    try:
        db.write("five", 6, False)
    except ValueError:
        pass
    else:
        assert False, "Expected write failure."
    db.delete("five")
    assert db.list() == []


def test_KeyDB(tmp_Path) -> None:
    # KeyDB is an alias for BlobKeyDB; wrap in JsonKeyDB for JSON round-trips.
    with KeyDBWrapper(tmp_Path) as db:
        keydb_generic_test(db, 5)


def test_KeyDBWindow(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        keydb_generic_test(window, 5)


def test_KeyDBFactory_same(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: int(data))
        keydb_generic_test(factory, 5)


def test_KeyDBFactory_diff(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: str(data))
        keydb_generic_test(factory, "5")


# --- BlobKeyDB layer tests ---

def test_blobkeydb_read_missing(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        with pytest.raises(FileNotFoundError):
            db.read("absent")


def test_blobkeydb_write_overwrite_raises(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("k", egest(keydb_encoder.encode(1)), False)
        with pytest.raises(ValueError):
            db.write("k", egest(keydb_encoder.encode(2)), False)


def test_blobkeydb_verify_ok(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("k", egest(keydb_encoder.encode({"x": 1})), False)
        assert db.verify("k") is True


def test_blobkeydb_verify_corrupt(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("k", egest(keydb_encoder.encode({"x": 1})), False)
        key_csum = db._key_blob("k")
        # Corrupt the blob content
        assert db.bs is not None
        with db.bs.session() as sess:
            sess.import_via_fd(lambda: BytesIO(b'corrupt data'), key_csum, force=True)
        assert db.verify("k") is False


def test_blobkeydb_verify_missing(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        with pytest.raises(FileNotFoundError):
            db.verify("absent")


def test_blobkeydb_checksum_blob_backed(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        value = egest(keydb_encoder.encode({"x": 1}))
        db.write("k", value, False)
        csum = db.checksum("k")
        from hashlib import md5
        assert csum == md5(value).hexdigest()


def test_blobkeydb_checksum_file_backed(tmp_Path) -> None:
    from hashlib import md5
    from farmfs.util import egest
    from farmfs.keydb import keydb_encoder
    keydir = tmp_Path.join("keys")
    tmpdir = tmp_Path.join("tmp")
    blobdir = tmp_Path.join("blobs")
    keydir.mkdir()
    tmpdir.mkdir()
    blobdir.mkdir()
    from farmfs.blobstore import FileBlobstore
    bs = FileBlobstore(blobdir, tmpdir)
    db = BlobKeyDB(keydir, tmpdir, bs)
    value_bytes = egest(keydb_encoder.encode("hello"))
    csum = md5(value_bytes).hexdigest()
    key_path = db.keypath("legacykey")
    with key_path.open("wb") as f:
        f.write(value_bytes + b"\n")
        f.write(csum.encode("utf-8") + b"\n")
    assert db.checksum("legacykey") == csum


def test_blobkeydb_checksum_missing(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        with pytest.raises(FileNotFoundError):
            db.checksum("absent")


def test_blobkeydb_live_blobs(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("k1", egest(keydb_encoder.encode("val1")), False)
        db.write("k2", egest(keydb_encoder.encode("val2")), False)
        blobs = set(db.live_blobs())
        assert len(blobs) == 2


# --- JsonKeyDB layer tests ---

def test_jsonkeydb_roundtrip_ok(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        db.write("k", {"b": 1, "a": 2}, False)
        assert db.verify("k") is True


def test_jsonkeydb_roundtrip_fail(tmp_Path) -> None:
    """Write non-canonical JSON bytes directly into BlobKeyDB; JsonKeyDB.verify should fail."""
    with KeyDBWrapper(tmp_Path) as db:
        # Write canonical JSON via JsonKeyDB
        db.write("k", {"a": 1}, False)
        # Now overwrite the blob with non-canonical JSON (unsorted keys)
        non_canonical = b'{"b":1,"a":1}'
        csum = db.db._key_blob("k")
        assert db.db.bs is not None
        with db.db.bs.session() as sess:
            sess.import_via_fd(lambda: BytesIO(non_canonical), csum, force=True)
        assert db.verify("k") is False


def test_jsonkeydb_read_missing(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        with pytest.raises(FileNotFoundError):
            db.read("absent")


# --- KeyDBWindow verify passthrough ---

def test_window_verify_passthrough(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("ns", db)
        window.write("k", 42, False)
        assert window.verify("k") is True


# --- KeyDBFactory validate ---

def test_factory_validate_called(tmp_Path) -> None:
    validate_calls = []

    def my_validate(key, value):
        validate_calls.append((key, value))
        return []  # no errors

    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: int(data), validate=my_validate)
        factory.write("k", 7, False)
        result = factory.verify("k")
        assert result is True
        assert len(validate_calls) == 1
        assert validate_calls[0] == ("k", 7)


def test_factory_validate_errors(tmp_Path) -> None:
    def bad_validate(key, value):
        return ["something wrong"]

    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: int(data), validate=bad_validate)
        factory.write("k", 7, False)
        assert factory.verify("k") is False


# --- Legacy file-backed key read ---

def test_keydb_legacy_file_read(tmp_Path) -> None:
    """BlobKeyDB.read() should handle the old two-line file format (value\\nchecksum)."""
    from hashlib import md5
    from farmfs.util import egest
    from farmfs.keydb import keydb_encoder

    keydir = tmp_Path.join("keys")
    tmpdir = tmp_Path.join("tmp")
    blobdir = tmp_Path.join("blobs")
    keydir.mkdir()
    tmpdir.mkdir()
    blobdir.mkdir()
    bs = FileBlobstore(blobdir, tmpdir)
    db = BlobKeyDB(keydir, tmpdir, bs)

    # Write a legacy-format file manually
    value_bytes = egest(keydb_encoder.encode("hello"))
    csum = md5(value_bytes).hexdigest()
    key_path = db.keypath("legacykey")
    # keydir already exists; write directly under it
    with key_path.open("wb") as f:
        f.write(value_bytes + b"\n")
        f.write(csum.encode("utf-8") + b"\n")

    result = db.read("legacykey")
    assert result == value_bytes


# --- BlobKeyDB.list glob pattern tests ---

def test_blobkeydb_list_empty(tmp_Path) -> None:
    with BlobKeyDBWrapper(tmp_Path) as db:
        assert db.list() == []
        assert db.list("**") == []
        assert db.list("*.txt") == []


def test_blobkeydb_list_default_returns_all(tmp_Path) -> None:
    """list() with no args returns all keys as relative strings."""
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("alpha", egest(keydb_encoder.encode(1)), False)
        db.write("beta", egest(keydb_encoder.encode(2)), False)
        result = db.list()
        assert sorted(result) == ["alpha", "beta"]
        # Results are plain strings, not Path objects
        assert all(isinstance(k, str) for k in result)


def test_blobkeydb_list_nested_keys_are_relative_strings(tmp_Path) -> None:
    """Keys in subdirectories are returned as relative path strings (no leading slash)."""
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("ns/alpha", egest(keydb_encoder.encode(1)), False)
        db.write("ns/beta", egest(keydb_encoder.encode(2)), False)
        db.write("other/gamma", egest(keydb_encoder.encode(3)), False)
        result = sorted(db.list())
        assert result == ["ns/alpha", "ns/beta", "other/gamma"]
        assert all(not k.startswith("/") for k in result)


def test_blobkeydb_list_prefix_glob(tmp_Path) -> None:
    """A prefix glob like 'ns/**' returns only keys under that namespace."""
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("ns/alpha", egest(keydb_encoder.encode(1)), False)
        db.write("ns/beta", egest(keydb_encoder.encode(2)), False)
        db.write("other/gamma", egest(keydb_encoder.encode(3)), False)
        result = sorted(db.list("ns/**"))
        assert result == ["ns/alpha", "ns/beta"]


def test_blobkeydb_list_wildcard_pattern(tmp_Path) -> None:
    """A wildcard like 'ns/al*' matches only the matching key."""
    with BlobKeyDBWrapper(tmp_Path) as db:
        from farmfs.util import egest
        from farmfs.keydb import keydb_encoder
        db.write("ns/alpha", egest(keydb_encoder.encode(1)), False)
        db.write("ns/beta", egest(keydb_encoder.encode(2)), False)
        result = db.list("ns/al*")
        assert result == ["ns/alpha"]


# --- KeyDBWindow.list glob pattern tests ---

def test_window_list_default_returns_local_keys(tmp_Path) -> None:
    """KeyDBWindow.list() returns keys relative to the window prefix, as plain strings."""
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("ns", db)
        window.write("alpha", 1, False)
        window.write("beta", 2, False)
        result = sorted(window.list())
        assert result == ["alpha", "beta"]
        assert all(isinstance(k, str) for k in result)
        assert all(not k.startswith("/") for k in result)


def test_window_list_does_not_see_other_namespaces(tmp_Path) -> None:
    """Keys outside the window prefix are not returned."""
    with KeyDBWrapper(tmp_Path) as db:
        window_a = KeyDBWindow("a", db)
        window_b = KeyDBWindow("b", db)
        window_a.write("key1", 1, False)
        window_b.write("key2", 2, False)
        assert window_a.list() == ["key1"]
        assert window_b.list() == ["key2"]


def test_window_list_wildcard_pattern(tmp_Path) -> None:
    """KeyDBWindow.list('al*') returns only matching keys, stripped of prefix."""
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("ns", db)
        window.write("alpha", 1, False)
        window.write("beta", 2, False)
        result = window.list("al*")
        assert result == ["alpha"]


# --- validate_snapshot ordering tests ---

def _make_snap(items: list) -> KeySnapshot:
    """Build a KeySnapshot from a list of (path, type) tuples."""
    def fake_reverser(x: str) -> str:
        return x
    data = [{"path": path, "type": t, "csum": "abc123"} if t == "link" else {"path": path, "type": t} for path, t in items]
    return KeySnapshot(data, "test", fake_reverser)


def test_validate_snapshot_clean(tmp_Path) -> None:
    """A snapshot with correctly ordered entries reports no errors."""
    # 'dir (extra)' sorts after 'dir/' entries in SnapshotItem order
    snap = _make_snap([
        ("dir", "dir"),
        ("dir/file.mp3", "link"),
        ("dir (extra)", "dir"),
        ("dir (extra)/file.mp3", "link"),
    ])
    errors = validate_snapshot("mysnap", snap)
    assert errors == []


def test_validate_snapshot_paren_vs_slash(tmp_Path) -> None:
    """
    Flat string sort puts 'dir (extra)' before 'dir/file' because '(' < '/'.
    SnapshotItem ordering puts 'dir/file' before 'dir (extra)' because
    path components are compared depth-first.
    validate_snapshot must use SnapshotItem ordering, not flat string sort.
    """
    # Provide items already in SnapshotItem order — should be clean.
    snap = _make_snap([
        ("dir", "dir"),
        ("dir/file.mp3", "link"),
        ("dir (extra)", "dir"),
        ("dir (extra)/file.mp3", "link"),
    ])
    errors = validate_snapshot("mysnap", snap)
    assert errors == [], f"False positive: {errors}"


# --- Legacy absolute-path normalisation ---

def test_snapshot_item_normalises_root() -> None:
    """SnapshotItem normalises '/' to '.' on construction."""
    from farmfs.snapshot import SnapshotItem
    item = SnapshotItem("/", "dir")
    assert item._path == "."


def test_snapshot_item_normalises_absolute_path() -> None:
    """SnapshotItem strips leading '/' from absolute paths on construction."""
    from farmfs.snapshot import SnapshotItem
    item = SnapshotItem("/foo/bar", "dir")
    assert item._path == "foo/bar"


def test_snapshot_item_relative_unchanged() -> None:
    """SnapshotItem leaves already-relative paths untouched."""
    from farmfs.snapshot import SnapshotItem
    item = SnapshotItem("foo/bar", "dir")
    assert item._path == "foo/bar"


def test_snapshot_item_get_dict_canonical() -> None:
    """get_dict() emits the normalised path, so JSON round-trip detects legacy snaps."""
    from farmfs.snapshot import SnapshotItem
    item = SnapshotItem("/foo", "dir")
    assert item.get_dict() == {"path": "foo", "type": "dir"}
