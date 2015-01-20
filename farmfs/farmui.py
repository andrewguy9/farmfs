import farmfs
from docopt import docopt

USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap (make|list|read|delete|restore) <snap>
  farmfs fsck
  farmfs count
  farmfs similarity
  farmfs gc
  farmfs checksum <path>...
  farmfs remote add <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list
  farmfs pull <remote> [<snap>]


Options:

"""

def empty2dot(paths):
  if len(paths) == 0:
    return ["."]
  else:
    return paths

def main():
  args = docopt(USAGE)
  if args['mkfs']:
    farmfs.mkfs('.')
  elif args['status']:
    farmfs.status(empty2dot(args['<path>']))
  elif args['freeze']:
    farmfs.freeze(empty2dot(args['<path>']))
  elif args['thaw']:
    farmfs.thaw(empty2dot(args['<path>']))
  elif args['fsck']:
    farmfs.fsck()
  elif args['count']:
    farmfs.count()
  elif args['similarity']:
    farmfs.similarity()
  elif args['gc']:
    farmfs.gc()
  elif args['snap']:
    snap_verbs = "make list read delete restore".split(" ")
    verb = snap_verbs[map(args.get, snap_verbs).index(True)]
    farmfs.snap(verb, args['<snap>'])
  elif args['checksum']:
    farmfs.checksum(args['<path>'])
  elif args['remote']:
    remote_verbs = "add remove list".split(" ")
    if args["add"]:
      farmfs.remote_add(args['<remote>'], args['<root>'])
    elif args["remove"]:
      farmfs.remote_remove(args['<remote>'])
    elif args["list"]:
      farmfs.remote_list()
  elif args['pull']:
    farmfs.pull(args['<remote>'], args['<snap>'])
