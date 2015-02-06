from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.fs import Path
from farmfs.fs import ensure_absent

class KeyDBWrapper:
  def __init__(self, root):
    self.root = Path(root)
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
    factory = KeyDBFactory(window, int)
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
    factory = KeyDBFactory(window, str)
    assert factory.list() == []
    factory.write("five", 5)
    assert factory.list() == ["five"]
    value = factory.read("five")
    assert value == str(5)
    factory.delete("five")
    assert factory.list() == []

