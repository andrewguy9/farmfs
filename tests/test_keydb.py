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

def test_KeyDB(tmp_Path):
  with KeyDBWrapper(tmp_Path) as db:
    assert db.list() == []
    db.write("five", 5)
    assert db.list() == ["five"]
    value = db.read("five")
    assert value == 5
    db.delete("five")
    assert db.list() == []

def test_KeyDBWindow(tmp_Path):
  with KeyDBWrapper(tmp_Path) as db:
    window = KeyDBWindow("window", db)
    assert window.list() == []
    window.write("five", 5)
    assert window.list() == ["five"]
    value = window.read("five")
    assert value == 5
    window.delete("five")
    assert window.list() == []

def test_KeyDBFactory_same(tmp_Path):
  with KeyDBWrapper(tmp_Path) as db:
    window = KeyDBWindow("window", db)
    factory = KeyDBFactory(window, str, lambda data, name: int(data))
    assert factory.list() == []
    factory.write("five", 5)
    assert factory.list() == ["five"]
    value = factory.read("five")
    assert value == 5
    factory.delete("five")
    assert factory.list() == []

def test_KeyDBFactory_diff(tmp_Path):
  with KeyDBWrapper(tmp_Path) as db:
    window = KeyDBWindow("window", db)
    factory = KeyDBFactory(window, str, lambda data, name : safetype(data))
    assert factory.list() == []
    factory.write("five", 5)
    assert factory.list() == ["five"]
    value = factory.read("five")
    assert value == safetype(5)
    factory.delete("five")
    assert factory.list() == []

def test_KeyDBFactory_copy(tmp_Path):
  with KeyDBWrapper(Path("db1", tmp_Path)) as db1:
    window1 = KeyDBWindow("window", db1)
    factory1 = KeyDBFactory(window1, str, lambda data, name : safetype(data))
    assert factory1.list() == []
    factory1.write("five", 5)
    with KeyDBWrapper(Path("db2", tmp_Path)) as db2:
      window2 = KeyDBWindow("other", db2)
      factory2 = KeyDBFactory(window2, str, lambda data, name: safetype(data))
      factory2.copy("five", window1)
      value = factory2.read("five")
      assert value == safetype(5)
