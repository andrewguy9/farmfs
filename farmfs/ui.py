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

def status(vol, context, path):
  for thawed in vol.thawed(path):
    print thawed.relative_to(context, leading_sep=False)

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
    #TODO prints root and data relative to /
    print "FileSystem Created %s using blobstore %s" % (root, data)
  else:
    vol = getvol(cwd)
    paths = map(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    if args['status']:
      #TODO prints thaw-ed paths relative to /.
      vol_status = partial(status, vol, cwd)
      map(vol_status, paths)
    elif args['freeze']:
      #TODO output feels unstructured.
      """
      Processing <root_path> with csum <full_udd_path>
      Found a copy of file already in userdata, skipping copy
      Putting link at <full_csum_path>
      """
      map(vol.freeze, paths)
    elif args['thaw']:
      #TODO no output?
      map(vol.thaw, paths)
    elif args['fsck']:
      #TODO Crashes on thawed value.
      #TODO ('CORRUPTION: checksum mismatch in ', /Users/andrewthomson/Downloads/farmtest/params/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e)
      for corruption in vol.fsck():
        exitcode = 1
        print corruption
    elif args['count']:
      #TODO 1 /d8e/8fc/a2d/c0f896fd7cb4cb0031ba249
      for f, c in vol.count().items():
        print c, f
    elif args['similarity']:
      #TODO 0.0 /other /sub
      for (dir_a, dir_b, sim) in vol.similarity():
        print sim, dir_a, dir_b
    elif args['gc']:
      #TODO Removing /0b6/d34/7b0/1d437a092be84c2edfce72c
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
          """
          mklink <leading_sep_vol_path> -> a1a/71f/4b4/6feaf72bf33627d78bbdc3e
          No need to copy blob, already exists
          mklink /jenny -> 812/a11/b49/b1a1cce5dd9a0018899501e
          No need to copy blob, already exists
          """
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
      #TODO output feels disordered.
      """
      mklink <leading_sep_vol_path> -> /a1a/71f/4b4/6feaf72bf33627d78bbdc3e
      Blob missing from local, copying
      Removing <leading_sep_vol_path>
      No need to copy blob, already exists
      """
      remotedb = vol.remotedb
      remote_vol = remotedb.read(args['<remote>'])
      snap_name = args['<snap>']
      if snap_name is None:
        remote_snap = remote_vol.tree()
      else:
        remote_snap = remote_vol.snapdb.read(snap_name)
      snap_pull(vol.root, vol.tree(), vol.udd, remote_snap, remote_vol.udd)
  exit(exitcode)
