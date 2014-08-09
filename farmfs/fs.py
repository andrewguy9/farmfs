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
from os.path import walk
from os.path import isdir
from shutil import copyfile

_BLOCKSIZE = 65536

def suffix(prefix, path):
  assert path.startswith(prefix + sep), "%s must start with %s" % (path, prefix)
  return path[len(prefix+1):]

def _normalized(path):
  if normalize(path) == path:
    return True
  else:
    return False

def ensure_dir(path):
  assert _normalized(path), path
  try:
    mkdir(path)
  except OSError as e:
    if e.errno == FileExists:
      pass
    elif e.errno == DirectoryExists:
      pass
    else:
      raise e

def normalize(path):
  return abspath(normpath(path))

def parents(path):
  assert _normalized(path), path
  path = abspath(path)
  parents = [path]
  while True:
    parent = split(path)[0]
    parents.append(parent)
    if parent == "/":
      return parents
    else:
      path = parent

def find_seq(name, seq):
  for i in seq:
    assert _normalized(i), i
    path = join(i, name)
    if exists(path):
      return path
  return None

def dir_gen(path):
  assert _normalized(path), path
  assert isdir(path), "%s is not a directory" % path
  names = listdir(path)
  for name in names:
    child_path = join(path, name)
    yield child_path

def checksum(path):
  assert _normalized(path), path
  hasher = md5()
  with open(path, 'rb') as fd:
    buf = fd.read(_BLOCKSIZE)
    while len(buf) > 0:
      hasher.update(buf)
      buf = fd.read(_BLOCKSIZE)
    return hasher.hexdigest()

def checksum_to_path(checksum, num_segs=3, seg_len=3):
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return join(*segs)

def import_file(path, userdata_path):
  assert _normalized(path), path
  assert _normalized(userdata_path), userdata_path
  dst = join(userdata_path, checksum_to_path(checksum(path)))
  parent, _ = split(dst)
  print "Creating indireciton dirs %s" % parent
  map(ensure_dir, reversed(parents(parent)))
  if exists(dst):
    print "Found a copy of file already in userdata, skipping copy"
  else:
    print "Putting link at %s" % dst
    link(path, dst)
  print "deleting %s" % path
  unlink(path)
  print "linking %s to %s" % (dst,path)
  symlink(dst, path)

def export_file(user_path):
  assert _normalized(user_path), user_path
  csum_path = readlink(user_path)
  unlink(user_path)
  copyfile(csum_path, user_path)

