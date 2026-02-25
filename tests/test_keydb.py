import pytest
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

    def __enter__(self):
        ensure_absent(self.root)
        self.root.mkdir()
        tmp = self.root.join("tmp")
        tmp.mkdir()
        db_root = self.root.join("keys")
        db_root.mkdir()
        return KeyDB(db_root, tmp)

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
        key_path = db.keypath("mykey")
        data, csum = db.readparts("mykey")
        db.writeraw(key_path, data, "deadbeefdeadbeefdeadbeefdeadbeef")
        results = list(db.iter_raw())
        assert len(results) == 1
        key, _, stored, ok = results[0]
        assert key == "mykey"
        assert stored == "deadbeefdeadbeefdeadbeefdeadbeef"
        assert ok is False


def test_keydb_iter_raw_ok(tmp_Path) -> None:
    with KeyDBWrapper(tmp_Path) as db:
        db.write("mykey", {"x": 1}, False)
        results = list(db.iter_raw())
        assert len(results) == 1
        key, _, _, ok = results[0]
        assert key == "mykey"
        assert ok is True
