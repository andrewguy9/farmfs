from os import mkdir
from os import listdir
from os import link
from os import unlink
from os import symlink
from os import readlink
from os import rmdir
from os import stat
from os import chmod
from errno import EEXIST as FileExists
from errno import EISDIR as DirectoryExists
from hashlib import md5
from os.path import stat as statc
from os.path import normpath
from os.path import split
from os.path import abspath
from os.path import exists
from os.path import isdir
from shutil import copyfile
from os.path import isfile, islink, sep
from func_prototypes import typed, returned

_BLOCKSIZE = 65536

@returned(basestring)
@typed(basestring)
def _normalize(path):
  return abspath(normpath(_decodePath(path)))

@returned(basestring)
@typed(basestring)
def _decodePath(name):
  if type(name) == str: # leave unicode ones alone
    try:
      name = name.decode('utf8')
    except:
      name = name.decode('windows-1252')
  return name

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

  # Returns the parent of self. If self is root ('/'), parent returns None.
  # You much check the output of parent before using the value.
  # Notcie that parent of root in the shell is '/', so this is a semantic difference
  # between us and POSIX.
  def parent(self):
    if self._path == sep:
      return None
    else:
      return Path(split(self._path)[0])

  def parents(self):
    paths = [self]
    path = self
    parent = path.parent()
    while parent is not None:
      paths.append(parent)
      path = parent
      parent = path.parent()
    return reversed(paths)

  def relative_to(self, relative, leading_sep=True):
    assert isinstance(relative, Path)
    parents = list(self.parents())
    assert relative in parents, "%s not in %s" % (relative, parents)
    relative_str = relative._path
    if leading_sep == True:
      prefix = sep
    else:
      prefix = ""
    return prefix + self._path[len(relative_str)+1:]

  def exists(self):
    return exists(self._path)

  def readlink(self):
    return Path(readlink(self._path))

  def link(self, dst):
    assert isinstance(dst, Path)
    link(dst._path, self._path)

  def symlink(self, dst):
    assert isinstance(dst, Path)
    symlink(dst._path, self._path)

  def copy(self, dst):
    assert isinstance(dst, Path)
    copyfile(self._path, dst._path)

  #TODO I'd like to have a way of cleaning up
  # empty directories recursvely, only if this
  # unlink succeeds.
  def unlink(self, clean=None):
    unlink(self._path)
    if clean is not None:
      parent = self.parent()
      parent._cleanup(clean)

  def rmdir(self, clean=None):
    rmdir(self._path)
    if clean is not None:
      parent = self.parent()
      parent._cleanup(clean)

  """Called on the parent of file or directory after a removal
  (if cleanup as asked for). Recuses cleanup until it reaches terminus.
  """
  def _cleanup(self, terminus):
    assert isinstance(terminus, Path)
    assert terminus in self.parents()
    if self == terminus:
      return
    if len(list(self.dir_gen())) == 0:
      self.rmdir(terminus)

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
    output = Path( self._path + sep + child)
    return output

  #TODO Should this also be able to generate raw basestrings?
  """Generates the set of Paths under this directory"""
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

  def stat(self):
    return stat(self._path)

  def chmod(self, mode):
    return chmod(self._path, mode)

@returned(bool)
@typed(Path)
def target_exists(link):
  assert link.islink()
  target = link.readlink()
  return target.exists()

def find_in_seq(name, seq):
  assert isinstance(name, basestring)
  for i in seq:
    assert isinstance(i, Path)
    path = i.join(name)
    if path.exists():
      return path
  return None

@typed(Path)
def ensure_absent(path):
  if path.exists():
    if path.isdir():
      for child in path.dir_gen():
        ensure_absent(child)
      path.rmdir()
    else:
      path.unlink()
  else:
    pass # No work to do.

@typed(Path)
def ensure_dir(path):
  if path.exists():
    if path.isdir():
      pass # There is nothing to do.
    else:
      path.unlink()
      path.mkdir()
  else:
    assert path != _ROOT, "Path is root, which must be a directory"
    parent = path.parent()
    assert parent != path, "Path and parent were the same!"
    ensure_dir(parent)
    path.mkdir()

@typed(Path, Path)
def ensure_link(path, orig):
  assert orig.exists()
  parent = path.parent()
  assert parent != path, "Path and parent were the same!"
  ensure_dir(parent)
  ensure_absent(path)
  path.link(orig)

@typed(Path)
def ensure_readonly(path):
  mode = path.stat().st_mode
  read_only = mode & ~statc.S_IWUSR & ~statc.S_IWGRP & ~statc.S_IWOTH
  path.chmod(read_only)

@typed(Path, Path)
def ensure_copy(path, orig):
  assert orig.exists()
  parent = path.parent()
  assert parent != path, "Path and parent were the same!"
  ensure_dir(parent)
  ensure_absent(path)
  print "***", path, orig
  orig.copy(path)

@typed(Path, Path)
def ensure_symlink(path, orig):
  assert orig.exists()
  parent = path.parent()
  assert parent != path, "Path and parent were the same!"
  ensure_dir(parent)
  ensure_absent(path)
  path.symlink(orig)

"""
Creates/Deletes directories. Does whatever is required inorder
to make and open a file with the mode previded.

Mode settings to consider are:
 O_CREAT         create file if it does not exist
 O_TRUNC         truncate size to 0
 O_EXCL          error if O_CREAT and the file exists
"""
def ensure_file(path, mode):
  assert isinstance(path, Path)
  parent = path.parent()
  assert parent != path, "Path and parent were the same!"
  ensure_dir(parent)
  fd = path.open(mode)
  return fd

_ROOT = Path(sep)

