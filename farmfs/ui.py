import farmfs
from snapshot import snap_pull
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs.util import empty2dot
from farmfs.volume import mkfs
from os import getcwdu
from fs import Path

USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap (make|read|delete|restore) <snap>
  farmfs snap list
  farmfs fsck
  farmfs count
  farmfs similarity
  farmfs gc
  farmfs remote add <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list [<remote>]
  farmfs pull <remote> [<snap>]


Options:

"""

#TODO This function returns absolute paths.
#It would be easier for the user if it was relative to CWD.
# That is blocked on Path.relative_to supporting complex relationships.
def status(vol, path):
  for thawed in vol.thawed(path):
    print thawed

def main():
  args = docopt(USAGE)
  exitcode = 0
  cwd = Path(getcwdu())
  if args['mkfs']:
    root = Path(args['<root>'] or ".", cwd)
    if args['<data>']:
      data = Path(args['<data>'], cwd)
    else:
      data = Path(".farmfs/userdata", root)
    mkfs(root, data)
    print "FileSystem Created %s using blobstore %s" % (root, data)
  else:
    vol = getvol(cwd)
    paths = map(lambda x: Path(x, cwd), empty2dot(args['<path>']))
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
        print "\n".join(snapdb.list())
      else:
        name = args['<snap>']
        if args['make']:
          snapdb.write(name, vol.tree())
        elif args['read']:
          snap = snapdb.read(name)
          for i in snap:
            print i
        elif args['delete']:
          snapdb.delete(name)
        elif args['restore']:
          snap = snapdb.read(name)
          tree = vol.tree()
          snap_pull(vol.root, tree, vol.udd, snap, vol.udd)
    elif args['remote']:
      remotedb = vol.remotedb
      if args["add"]:
        remote_vol = getvol(Path(args['<root>'], cwd))
        remotedb.write(args['<remote>'], remote_vol)
      elif args["remove"]:
        remotedb.delete(args['<remote>'])
      elif args["list"]:
        if args["<remote>"]:
          remote_vol = remotedb.read(args['<remote>'])
          print "\n".join(remote_vol.snapdb.list())
        else:
          for remote in remotedb.list():
            print remote
    elif args['pull']:
      remotedb = vol.remotedb
      remote_vol = remotedb.read(args['<remote>'])
      snap_name = args['<snap>']
      if snap_name is None:
        remote_snap = remote_vol.tree()
      else:
        remote_snap = remote_vol.snapdb.read(snap_name)
      snap_pull(vol.root, vol.tree(), vol.udd, remote_snap, remote_vol.udd)
  exit(exitcode)
