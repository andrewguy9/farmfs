from keydb import KeyDB, KeyDBWindow
from farmfs import getvol

REMOTE_PATH="remotes"
class RemoteDatabase:
  def __init__(self, keydb):
    assert isinstance(keydb, KeyDB)
    self.window = KeyDBWindow(REMOTE_PATH, keydb)

  def list(self):
    return self.window.list()

  def delete(self, name):
    self.window.delete(name)

  def save(self, name, remote):
    assert isinstance(remote, FarmFSVolume)
    root = str(remote.root()) # TODO THIS MUST BE ABSOLUTE PATH...
    self.window.write(name, root)

  @returned(FarmFSVolume)
  def get(self, name):
    assert isinstance(name, basestring)
    remote_location = self.window.read(name)
    return getvol(remote_location)

