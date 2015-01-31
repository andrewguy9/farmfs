import farmfs
from snapshot import snap_pull
from farmfs import getvol
from farmfs import makePath
from docopt import docopt
from functools import partial
from farmfs.util import empty2dot
from farmfs.volume import mkfs

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
    mkfs(cwd)
    print "FileSystem Created %s" % cwd
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
      snapdb = vol.snapdb
      if args['list']:
        for snap in snapdb.list():
          print snap
      else:
        name = args['<snap>']
        if args['make']:
          vol.snap(name)
        elif args['read']:
          snap = snapdb.get(name)
          for i in snap:
            print i
        elif args['delete']:
          snapdb.delete(name)
        elif args['restore']:
          snap = snapdb.get(name)
          tree = vol.tree()
          snap_pull(vol.root(), tree, vol.udd, snap, vol.udd)
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
