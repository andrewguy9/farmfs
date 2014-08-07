from fs import ensure_dir
from os import sep

###########
# HELPERS #
###########

def userdata_path(root):
  return root + sep + ".userdata"

def metadata_path(root):
  return root + sep + ".metadata"

def keys_path(root):
  return metadata_path(root) + sep + "keys"

###########
# OBJECTS #
###########

class VolumeContext:
  pass

#########
# VERBS #
#########

def mkfs(args):
  ensure_dir(args.root)
  ensure_dir(userdata_path(args.root))
  ensure_dir(metadata_path(args.root))
  ensure_dir(keys_path(args.root))
  print "FileSystem Created!"

