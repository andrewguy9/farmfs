import pytest
from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.fs import Path
from farmfs.fs import ensure_absent
from farmfs.util import safetype

@pytest.fixture()
def tmp_Path(tmp_path):
    return Path(str(tmp_path))

class KeyDBWrapper:
    def __init__(self, datadir):
        self.root = Path(datadir)

    def __enter__(self):
        ensure_absent(self.root)
        self.root.mkdir()
        return KeyDB(self.root)

    def __exit__(self, type, value, traceback):
        ensure_absent(self.root)

def keydb_generic_test(db, expected_value):
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

def test_KeyDB(tmp_Path):
    with KeyDBWrapper(tmp_Path) as db:
        keydb_generic_test(db, 5)

def test_KeyDBWindow(tmp_Path):
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        keydb_generic_test(window, 5)

def test_KeyDBFactory_same(tmp_Path):
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: int(data))
        keydb_generic_test(factory, 5)

def test_KeyDBFactory_diff(tmp_Path):
    with KeyDBWrapper(tmp_Path) as db:
        window = KeyDBWindow("window", db)
        factory = KeyDBFactory(window, str, lambda data, name: safetype(data))
        keydb_generic_test(factory, "5")
