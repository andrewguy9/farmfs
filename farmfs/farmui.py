import farmfs
from farmfs import getvol
from farmfs import makePath
from docopt import docopt
from functools import partial
from farmfs.util import empty2dot

USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap (make|read|delete|restore) <snap>
  farmfs snap list
  farmfs fsck
  farmfs count
  farmfs similarity
  farmfs gc
  farmfs remote add <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list
  farmfs pull <remote> [<snap>]


Options:

"""

def status(vol, path):
  for thawed in vol.thawed(path):
    print thawed

def main():
  args = docopt(USAGE)
  exitcode = 0
  cwd = makePath(".")
  if args['mkfs']:
    farmfs.mkfs(cwd)
  else:
    vol = getvol(cwd)
    paths = map(makePath, empty2dot(args['<path>']))
    if args['status']:
      vol_status = partial(status, vol)
      map(vol_status, paths)
    elif args['freeze']:
      map(vol.freeze, paths)
    elif args['thaw']:
      map(vol.thaw, paths)
    elif args['fsck']:
      for corruption in vol.fsck():
        exitcode = 1
        print corruption
    elif args['count']:
      for f, c in vol.count().items():
        print c, f
    elif args['similarity']:
      for (dir_a, dir_b, sim) in vol.similarity():
        print sim, dir_a, dir_b
    elif args['gc']:
      for f in farmfs.gc(vol):
        print "Removing", f
    elif args['snap']:
      snap_verbs = "make list read delete restore".split(" ")
      verb = snap_verbs[map(args.get, snap_verbs).index(True)]
      farmfs.snap(verb, args['<snap>'])
    elif args['remote']:
      remote_verbs = "add remove list".split(" ")
      if args["add"]:
        remote_vol = getvol(makePath(args['<root>']))
        farmfs.remote_add(vol, args['<remote>'], remote_vol)
      elif args["remove"]:
        farmfs.remote_remove(vol, args['<remote>'])
      elif args["list"]:
        farmfs.remote_list(vol)
    elif args['pull']:
      farmfs.pull(vol, args['<remote>'], args['<snap>'])
  exit(exitcode)
