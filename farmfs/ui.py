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
      remotedb = vol.remotedb
      if args["add"]:
        remote_vol = getvol(makePath(args['<root>']))
        remotedb.save(args['<remote>'], remote_vol)
      elif args["remove"]:
        remotedb.delete(args['<remote>'])
      elif args["list"]:
        farmfs.remote_list(vol)
        for remote in remotedb.list():
          print remote
    elif args['pull']:
      remotedb = vol.remotedb
      remote_vol = remotedb.get(args['<remote>'])
      remote_snap = remote_vol.snapdb.get(args['<snap>'])
      snap_pull(vol.root(), vol.tree(), vol.udd, remote_snap, remote_vol.udd)
  exit(exitcode)
