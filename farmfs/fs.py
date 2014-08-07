from os import mkdir
from errno import EEXIST as FileExists

def ensure_dir(path):
  try:

    mkdir(path)
  except OSError as e:
    if e.errno != FileExists:
      raise e
