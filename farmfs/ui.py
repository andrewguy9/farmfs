import farmfs
from snapshot import snap_pull
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs.util import empty2dot, fmap, transduce, concat
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
    print "FileSystem Created %s using blobstore %s" % (root, data)
  else:
    vol = getvol(cwd)
    paths = map(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    if args['status']:
      vol_status = partial(status, vol, cwd)
      map(vol_status, paths)
    elif args['freeze']:
      def printr(freeze_op):
        s = "Imported %s with checksum %s" % \
                (freeze_op['path'].relative_to(cwd, leading_sep=False),
                 freeze_op['csum'])
        if freeze_op['was_dup']:
          print s, "was a duplicate"
        else:
          print s
      importer = fmap(vol.freeze)
      get_thawed = fmap(vol.thawed)
      print_list = fmap(printr)
      transduce(get_thawed, concat, importer, print_list)(paths)
    elif args['thaw']:
      def printr(path):
        print "Exported %s" % path.relative_to(cwd, leading_sep=False)
      exporter = fmap(vol.thaw)
      get_frozen = fmap(vol.frozen)
      print_list = fmap(printr)
      transduce(get_frozen, concat, exporter, print_list)(paths)
    elif args['fsck']:
      def print_missing_blob(csum, items):
        print "CORRUPTION missing blob %s" % csum
        for item in items:
          props = item.get_dict()
          path = Path(props['path'], vol.root)
          snap = item._snap
          if snap:
            print "\t%s\t%s" % (snap, path.relative_to(cwd, leading_sep=False))
          else:
            print "\t%s"%path.relative_to(cwd, leading_sep=False)
      def print_checksum_mismatch(csum):
        print "CORRUPTION checksum mismatch in blob %s" % csum #TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
      missing_blobs = list(vol.check_links())
      mismatches = (vol.check_userdata_hashes())
      for missing_blob in missing_blobs:
          print_missing_blob(*missing_blob)
      for mismatch in mismatches:
          print_checksum_mismatch(mismatch)
      if len(missing_blobs) != 0:
          exitcode = 1
      if len(mismatches) != 0:
          exitcode = 2
    elif args['count']:
      for f, c in vol.count().items():
        print c, f
    elif args['similarity']:
      for (dir_a, count_a, dir_b, count_b, intersect) in vol.similarity():
        path_a = Path(dir_a, vol.root).relative_to(cwd, leading_sep=False)
        path_b = Path(dir_b, vol.root).relative_to(cwd, leading_sep=False)
        print path_a, "%d/%d %d%%" % (intersect, count_a, int(100*float(intersect)/count_a)), \
                path_b, "%d/%d %d%%" % (intersect, count_b, int(100*float(intersect)/count_b))
    elif args['gc']:
      #TODO crashes. refs vs csums.
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
