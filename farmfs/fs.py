from os import mkdir
from os import listdir
from os import link
from os import unlink
from os import symlink
from os import readlink
from os import rmdir
from os import stat
from os import chmod
from errno import ENOENT as FileDoesNotExist
from errno import EEXIST as FileExists
from errno import EISDIR as DirectoryExists
from hashlib import md5
from os.path import stat as statc
from os.path import normpath
from os.path import split
from os.path import isabs
from os.path import exists
from os.path import isdir
from shutil import copyfileobj
from os.path import isfile, islink, sep
from func_prototypes import typed, returned
from glob import fnmatch
from fnmatch import fnmatchcase
from functools import total_ordering, partial
from farmfs.util import ingest, safetype, uncurry, first
from future.utils import python_2_unicode_compatible
from safeoutput import open as safeopen
try:
    from itertools import ifilter
except ImportError:
    # On python3, filter is lazy.
    ifilter = filter


_BLOCKSIZE = 65536

LINK=u'link'
FILE=u'file'
DIR=u'dir'

TYPES=[LINK, FILE, DIR]

#TODO should take 1 arg, return fn.
def skip_ignored(ignored, path, ftype):
  for i in ignored:
    if fnmatchcase(path._path, i):
      return True
  return False

def ftype_selector(keep_types):
  keep = lambda p, ft: ft in keep_types # Take p and ft since we may want to use it in entries.
  entry_keep = uncurry(keep) # Expand tuple from entries.
  entry_filter = partial(ifilter, entry_keep)
  return entry_filter

@total_ordering
@python_2_unicode_compatible
class Path:
  def __init__(self, path, frame=None):
    if path is None:
      raise ValueError("path must be defined")
    elif isinstance(path, Path):
      assert frame is None
      self._path = path._path
    else:
      path = ingest(path)
      if frame is None:
        assert isabs(path), "Frame is required when building relative paths: %s" % path
        self._path = normpath(path)
      else:
        assert isinstance(frame, Path)
        assert not isabs(path), "path %s is required to be relative when a frame %s is provided" % (path, frame)
        self._path = frame.join(path)._path
    assert isinstance(self._path, safetype)

  def __str__(self):
    return self._path

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
      return Path(first(split(self._path)))

  def parents(self):
    paths = [self]
    path = self
    parent = path.parent()
    while parent is not None:
      paths.append(parent)
      path = parent
      parent = path.parent()
    return reversed(paths)

  #TODO This function returns leading '/' on relations.
  #TODO This function returns '/' for matches. It should return '.'
  #TODO This function doesn't handle "complex" relationships.
  #XXX This function leads to confusion. It returns a string when mostly you
  # want to mess with Paths. It should only be called in user output schenatios.
  #TODO Check where this is called and try to stop calling it.
  #TODO Rename this to somthing which disourages use.
  #TODO Rename this so the string return value is called out.
  def relative_to(self, relative, leading_sep=True):
    assert isinstance(relative, Path)
    if leading_sep == True:
      prefix = sep
    else:
      prefix = ""
    self_parents = list(self.parents())
    if relative in self_parents:
      relative_str = relative._path
      return prefix + self._path[len(relative_str)+1:]
    relative_parents = list(reversed(list(relative.parents())))
    if self in relative_parents:
      backups = relative_parents.index(self) - 1
      assert backups >= 0
      assert leading_sep == False, "Leading seperator is meaningless with backtracking"
      return sep.join([".."]*backups)
    raise ValueError("Relationship between %s and %s is complex" % (self, relative))


  def exists(self):
    return exists(self._path)

  def readlink(self, frame=None):
    return Path(readlink(self._path), frame)

  def link(self, dst):
    assert isinstance(dst, Path)
    link(dst._path, self._path)

  def symlink(self, dst):
    assert isinstance(dst, Path)
    symlink(dst._path, self._path)

  def copy(self, dst):
    assert isinstance(dst, Path)
    with open(self._path, 'rb') as src_fd:
      with safeopen(dst._path, 'wb') as dst_fd:
        copyfileobj(src_fd, dst_fd)

  def unlink(self, clean=None):
    try:
      unlink(self._path)
    except OSError as e:
      if e.errno == FileDoesNotExist:
        pass
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
      digest = safetype(hasher.hexdigest())
      return digest

  def __cmp__(self, other):
    return (self > other) - (self < other)

  def __eq__(self, other):
    assert isinstance(other, Path)
    return self._path == other._path

  def __ne__(self, other):
    assert isinstance(other, Path)
    return not (self == other)

  def __lt__(self, other):
    assert isinstance(other, Path)
    return self._path.split(sep) < other._path.split(sep)

  def __hash__(self):
    return hash(self._path)

  def join(self, child):
    child = safetype(child)
    try:
      output = Path( self._path + sep + child)
    except UnicodeDecodeError as e:
      raise ValueError(str(e) + "\nself path: "+ self._path + "\nchild: ", child)
    return output

  def dir_gen(self):
    """Generates the set of Paths under this directory"""
    assert self.isdir(), "%s is not a directory" % self._path
    assert isinstance(self._path, safetype)
    names = listdir(self._path)
    for name in names:
      child = self.join(name)
      yield child

  def ftype(self):
    if self.islink():
      return LINK
    elif self.isfile():
      return FILE
    elif self.isdir():
      return DIR
    else:
      raise ValueError("%s is not in %s" % (self, types))

  def entries(self, skip=None):
    t = self.ftype()
    if skip and skip(self, t):
      return
    yield (self, t)
    if t == DIR:
      children = self.dir_gen()
      for dir_entry in sorted(children):
        for x in dir_entry.entries(skip):
          yield x

  def open(self, mode):
    return open(self._path, mode)

  def stat(self):
    return stat(self._path)

  def chmod(self, mode):
    return chmod(self._path, mode)

@returned(Path)
def userPath2Path(arg, frame):
    """
    Building paths using conventional POSIX systems will discard CWD if the
    path is absolute. FarmFS makes passing of CWD explicit so that path APIs
    are pure functions. Additionally FarmFS path construction doesn't allow
    for absolute paths to be mixed with frames. This is useful for
    spotting bugs and making sure that pathing has strong guarantees. However
    this comes at the expense of user expectation. When dealing with user
    input, there is an expecation that POSIX semantics are at play.
    userPath2Path checks to see if the provided path is absolute, and if not,
    adds the CWD frame.
    """
    arg = ingest(arg)
    if isabs(arg):
      return Path(arg)
    else:
      return Path(arg, frame)

@returned(bool)
@typed(Path)
def target_exists(link):
  assert link.islink()
  target = link.readlink(link.parent())
  return target.exists()

#TODO this function is dangerous. Would be better if we did sorting in the snaps to ensure order of ops explicitly.
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
    assert path != ROOT, "Path is root, which must be a directory"
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
  orig.copy(path)

@typed(Path, Path)
def ensure_symlink(path, orig):
  assert orig.exists()
  ensure_symlink_unsafe(path, orig._path)

@typed(Path, safetype)
def ensure_symlink_unsafe(path, orig):
  parent = path.parent()
  assert parent != path, "Path and parent were the same!"
  ensure_dir(parent)
  ensure_absent(path)
  symlink(orig, path._path)

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

ROOT = Path(sep)

