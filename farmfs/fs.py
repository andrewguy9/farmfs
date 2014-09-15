from os import mkdir
from os import listdir
from os import link
from os import unlink
from os import symlink
from os import readlink
from errno import EEXIST as FileExists
from errno import EISDIR as DirectoryExists
from hashlib import md5
from os.path import normpath
from os.path import split
from os.path import abspath
from os.path import join
from os.path import exists
from os.path import isdir
from shutil import copyfile
from os.path import isfile, islink, sep

_BLOCKSIZE = 65536

def _normalize(path):
  assert isinstance(path, basestring)
  return abspath(normpath(_decodePath(path)))

def _decodePath(name):
  assert isinstance(name, basestring)
  if type(name) == str: # leave unicode ones alone
    try:
      name = name.decode('utf8')
    except:
      name = name.decode('windows-1252')
  return name

def _checksum_to_path(checksum, num_segs=3, seg_len=3):
  assert isinstance(checksum, basestring)
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return join(*segs)

class Path:
  def __init__(self, path):
    if isinstance(path, basestring):
      self._path = _normalize(path)
    else:
      self._path = path._path

  def __unicode__(self):
    return u'%s' % self._path

  def __str__(self):
    return unicode(self).encode('utf-8')

  def __repr__(self):
    return str(self)

  def mkdir(self):
    try:
      mkdir(self._path)
    except OSError as e:
      if e.errno == FileExists:
        pass
      elif e.errno == DirectoryExists:
        pass
      else:
        raise e

  def parent(self):
    return Path(split(self._path)[0])

  def parents(self):
    parents = [self]
    path = self
    while True:
      parent = path.parent()
      parents.append(parent)
      if parent == Path(sep):
        return parents
      else:
        path = parent

  def relative_to(self, relative):
    assert isinstance(relative, Path)
    assert relative in self.parents(), "%s not in %s" % (relative, str(self.parents()))
    relative_str = relative._path + sep
    assert self._path.startswith(relative_str)
    return self._path[len(relative_str):]

  def exists(self):
    return exists(self._path)

  def readlink(self):
    return Path(readlink(self._path))

  def link(self, dst):
    assert isinstance(dst, Path)
    link(self._path, dst._path)

  def symlink(self, dst):
    assert isinstance(dst, Path)
    symlink(dst._path, self._path)

  def copy(self, dst):
    assert isinstance(dst, Path)
    copyfile(self._path, dst._path)

  def unlink(self):
    unlink(self._path)

  def islink(self):
    return islink(self._path)

  def isdir(self):
    return isdir(self._path)

  def isfile(self):
    return isfile(self._path)

  def checksum(self):
    hasher = md5()
    with self.open('rb') as fd:
      buf = fd.read(_BLOCKSIZE)
      while len(buf) > 0:
        hasher.update(buf)
        buf = fd.read(_BLOCKSIZE)
      return hasher.hexdigest()

  def __cmp__(self, other):
    assert isinstance(other, Path)
    self_parts = self._path.split(sep)
    other_parts = other._path.split(sep)
    return cmp(self_parts, other_parts)

  def __hash__(self):
    return hash(self._path)

  def join(self, child):
    assert isinstance(child, basestring)
    return Path( join(self._path, child) )

  def dir_gen(self):
    assert self.isdir(), "%s is not a directory" % self._path
    names = listdir(self._path)
    for name in names:
      child = self.join(name)
      yield child

  def entries(self, exclude=[]):
    if isinstance(exclude, Path):
      exclude = [exclude]
    for excluded in exclude:
      assert isinstance(excluded, Path)
    if self in exclude:
      pass
    elif self.islink():
      yield (self, "link")
    elif self.isfile():
      yield (self, "file")
    elif self.isdir():
      yield (self, "dir")
      for dir_entry in self.dir_gen():
        for x in dir_entry.entries(exclude):
          yield x
    else:
      raise ValueError("%s is not a file/dir/link" % self)

  def open(self, mode):
    return open(self._path, mode)

def validate_checksum(path):
  csum = path.checksum()
  return path._path.endswith(_checksum_to_path(csum)) #TODO DONT REFERENCE _PATH

def validate_link(path):
  link = path.readlink()
  assert isinstance(link, Path)
  return link.exists()

def find_in_seq(name, seq):
  assert isinstance(name, basestring)
  for i in seq:
    assert isinstance(i, Path)
    path = i.join(name)
    if path.exists():
      return path
  return None

def import_file(path, userdata_path):
  assert isinstance(path, Path)
  assert isinstance(userdata_path, Path)
  dst = userdata_path.join(_checksum_to_path(path.checksum()))
  #TODO HERE BEGINS RECURSIVE CREATION OF PARENTS
  parent = dst.parent()
  parents = parent.parents()
  print "Creating indireciton dirs %s" % parent
  for parent in reversed(parents):
    parent.mkdir()
  #TODO HERE ENDS RECURSIVE CREATION OF PARENTS
  if dst.exists():
    print "Found a copy of file already in userdata, skipping copy"
  else:
    print "Putting link at %s" % dst
    path.link(dst)
  print "deleting %s" % path
  path.unlink()
  print "linking %s to %s" % (dst,path)
  path.symlink(dst)

def export_file(user_path):
  assert isinstance(user_path, Path)
  csum_path = user_path.readlink()
  user_path.unlink()
  csum_path.copy(user_path)

