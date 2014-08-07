from os import mkdir
from errno import EEXIST as FileExists
from os.path import normpath
from os.path import split
from os.path import abspath
from os.path import join
from os.path import exists

def __normalized(path):
  if normalize(path) == path:
    return True
  else:
    return False

def ensure_dir(path):
  assert __normalized(path), path
  try:
    mkdir(path)
  except OSError as e:
    if e.errno != FileExists:
      raise e

def normalize(path):
  return abspath(normpath(path))

def parents(path):
  assert __normalized(path), path
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
    assert __normalized(i), i
    path = join(i, name)
    if exists(path):
      return path
  return None

