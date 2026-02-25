from io import BytesIO

import pytest
from farmfs.blobstore import FileBlobstore
from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBLike
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.fs import Path
from farmfs.fs import ensure_absent
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
        return KeyDB(self.keydir, self.tmpdir, bs)

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


def test_keydb_iter_raw_corrupt(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        db.write("mykey", {"x": 1}, False)
        key_blob = db.key_blob("mykey")
        db.bs.import_via_fd(lambda: BytesIO(b'corrupt data'), key_blob, force=True)
        results = list(db.iter_raw())
        assert len(results) == 1
        key, _, stored, ok = results[0]
        assert key == "mykey"
        assert ok is False


def test_keydb_iter_raw_ok(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        db.write("mykey", {"x": 1}, False)
        print(db.key_blob("mykey"))
        results = list(db.iter_raw())
        assert len(results) == 1
        key, _, _, ok = results[0]
        assert key == "mykey", results[0]
        assert ok is True, results[0]
