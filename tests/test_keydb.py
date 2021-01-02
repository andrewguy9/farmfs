from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.fs import Path
from farmfs import cwd
from farmfs.fs import ensure_absent
from farmfs.util import safetype

#TODO don't use cwd in sub-module.
#TODO lets use temp file fixture.
class KeyDBWrapper:
  def __init__(self, root):
    self.root = Path(root, cwd)
  def __enter__(self):
    ensure_absent(self.root)
    self.root.mkdir()
    return KeyDB(self.root)
  def __exit__(self, type, value, traceback):
    ensure_absent(self.root)

def test_KeyDB():
  with KeyDBWrapper("./db") as db:
    assert db.list() == []
    db.write("five", 5)
    assert db.list() == ["five"]
    value = db.read("five")
    assert value == 5
    db.delete("five")
    assert db.list() == []

def test_KeyDBWindow():
  with KeyDBWrapper("./db") as db:
    window = KeyDBWindow("window", db)
    assert window.list() == []
    window.write("five", 5)
    assert window.list() == ["five"]
    value = window.read("five")
    assert value == 5
    window.delete("five")
    assert window.list() == []

def test_KeyDBFactory_same():
  with KeyDBWrapper("./db") as db:
    window = KeyDBWindow("window", db)
    factory = KeyDBFactory(window, str, lambda data, name: int(data))
    assert factory.list() == []
    factory.write("five", 5)
    assert factory.list() == ["five"]
    value = factory.read("five")
    assert value == 5
    factory.delete("five")
    assert factory.list() == []

def test_KeyDBFactory_diff():
  with KeyDBWrapper("./db") as db:
    window = KeyDBWindow("window", db)
    factory = KeyDBFactory(window, str, lambda data, name : safetype(data))
    assert factory.list() == []
    factory.write("five", 5)
    assert factory.list() == ["five"]
    value = factory.read("five")
    assert value == safetype(5)
    factory.delete("five")
    assert factory.list() == []

def test_KeyDBFactory_copy():
  with KeyDBWrapper("./db1") as db1:
    window1 = KeyDBWindow("window", db1)
    factory1 = KeyDBFactory(window1, str, lambda data, name : safetype(data))
    assert factory1.list() == []
    factory1.write("five", 5)
    with KeyDBWrapper("./db2") as db2:
      window2 = KeyDBWindow("other", db2)
      factory2 = KeyDBFactory(window2, str, lambda data, name: safetype(data))
      factory2.copy("five", window1)
      value = factory2.read("five")
      assert value == safetype(5)
