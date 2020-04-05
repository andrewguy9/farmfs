from __future__ import print_function
import farmfs
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs import cwd
from farmfs.util import empty2dot, fmap, pipeline, concat, identify, uncurry, count, groupby, consume, concatMap, zipFrom, uncurry
from farmfs.volume import mkfs, tree_diff, tree_patcher
from farmfs.fs import Path, userPath2Path
try:
    from itertools import ifilter
except ImportError:
    # On python3, filter is lazy.
    ifilter = filter
import sys

USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap list
  farmfs snap (make|read|delete|restore|diff) <snap>
  farmfs fsck
  farmfs count
  farmfs similarity
  farmfs gc
  farmfs remote add <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list [<remote>]
  farmfs pull <remote> [<snap>]
  farmfs diff <remote> [<snap>]


Options:

"""

def op_doer(op):
    (blob_op, tree_op, desc) = op
    blob_op()
    tree_op()

stream_op_doer = fmap(op_doer)

def main():
    result = farmfs_ui(sys.argv[1:], cwd)
    exit(result)

def farmfs_ui(argv, cwd):
  args = docopt(USAGE, argv)
  exitcode = 0
  if args['mkfs']:
    root = userPath2Path(args['<root>'] or ".", cwd)
    if args['<data>']:
      data = userPath2Path(args['<data>'], cwd)
    else:
      data = Path(".farmfs/userdata", root)
    mkfs(root, data)
    print("FileSystem Created %s using blobstore %s" % (root, data))
  else:
    vol = getvol(cwd)
    paths = map(lambda x: userPath2Path(x, cwd), empty2dot(args['<path>']))
    def delta_printr(delta):
      deltaPath = delta.path(vol.root).relative_to(cwd, leading_sep=False)
      print("diff: %s %s %s" % (delta.mode, deltaPath, delta.csum))
    stream_delta_printr = fmap(identify(delta_printr))
    def op_printr(op):
      (blob_op, tree_op, (desc, path)) = op
      print(desc % path.relative_to(cwd, leading_sep=False))
    stream_op_printr = fmap(identify(op_printr))
    if args['status']:
      get_thawed = fmap(vol.thawed)
      pipeline(get_thawed,
              concat,
              fmap(lambda p: p.relative_to(cwd, leading_sep=False)),
              fmap(print),
              consume)(paths)
    elif args['freeze']:
      def printr(freeze_op):
        s = "Imported %s with checksum %s" % \
                (freeze_op['path'].relative_to(cwd, leading_sep=False),
                 freeze_op['csum'])
        if freeze_op['was_dup']:
          print(s, "was a duplicate")
        else:
          print(s)
      importer = fmap(vol.freeze)
      get_thawed = fmap(vol.thawed)
      print_list = fmap(printr)
      pipeline(get_thawed, concat, importer, print_list, consume)(paths)
    elif args['thaw']:
      def printr(path):
        print("Exported %s" % path.relative_to(cwd, leading_sep=False))
      exporter = fmap(vol.thaw)
      get_frozen = fmap(vol.frozen)
      print_list = fmap(printr)
      pipeline(get_frozen, concat, exporter, print_list, consume)(paths)
    elif args['fsck']:
      # Look for blobs in tree or snaps which are not in blobstore.
      trees = vol.trees()
      tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
      tree_links = partial(ifilter, lambda snap_item: snap_item[1].is_link())
      broken_tree_links = partial(
              ifilter,
              lambda snap_item: not vol.blob_checker(snap_item[1].csum()))
      checksum_grouper = partial(groupby,
              lambda snap_item: snap_item[1].csum())
      def broken_link_printr(csum, snap_items):
        print(csum)
        for (snap, item) in snap_items:
          print(
                  "\t",
                  snap.name,
                  vol.root.join(item.pathStr()).relative_to(cwd, leading_sep=False))
      broken_links_printr = fmap(identify(uncurry(broken_link_printr)))
      num_bad_blobs = pipeline(
              tree_items,
              tree_links,
              broken_tree_links,
              checksum_grouper,
              broken_links_printr,
              count)(trees)
      if num_bad_blobs != 0:
          exitcode = exitcode | 1

      # Look for checksum mismatches.
      def print_checksum_mismatch(csum):
        print("CORRUPTION checksum mismatch in blob %s" % csum)#TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
      select_broken = partial(ifilter, vol.check_userdata_blob)
      mismatches = pipeline(
        select_broken,
        fmap(vol.reverser),
        identify(fmap(print_checksum_mismatch)),
        count
        )(vol.userdata_files())
      if mismatches != 0:
          exitcode = exitcode | 2
    elif args['count']:
      trees = vol.trees()
      tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
      tree_links = partial(ifilter, lambda snap_item: snap_item[1].is_link())
      checksum_grouper = partial(groupby,
              lambda snap_item: snap_item[1].csum())
      def count_printr(csum, snap_items):
        print(csum, count(snap_items))
        for (snap, item) in snap_items:
            print(snap.name, vol.root.join(item.pathStr()).relative_to(cwd, leading_sep=False))
      counts_printr = fmap(identify(uncurry(count_printr)))
      pipeline(
              tree_items,
              tree_links,
              checksum_grouper,
              counts_printr,
              consume
              )(trees)
    elif args['similarity']:
      for (dir_a, count_a, dir_b, count_b, intersect) in vol.similarity():
        assert isinstance(dir_a, Path)
        assert isinstance(dir_b, Path)
        path_a = dir_a.relative_to(cwd, leading_sep=False)
        path_b = dir_b.relative_to(cwd, leading_sep=False)
        print(path_a, "%d/%d %d%%" % (intersect, count_a, int(100*float(intersect)/count_a)), \
                path_b, "%d/%d %d%%" % (intersect, count_b, int(100*float(intersect)/count_b)))
    elif args['gc']:
      for f in farmfs.gc(vol):
        print("Removing", f)
    elif args['snap']:
      snapdb = vol.snapdb
      if args['list']:
        #TODO have an optional argument for which remote.
        print("\n".join(snapdb.list()))
      else:
        name = args['<snap>']
        if args['delete']:
          snapdb.delete(name)
        elif args['make']:
          snapdb.write(name, vol.tree())
        else:
          snap = snapdb.read(name)
          if args['read']:
            for i in snap:
              print(i)
          elif args['restore']:
            tree = vol.tree()
            diff = tree_diff(vol.tree(), snap)
            pipeline(
                    stream_delta_printr,
                    tree_patcher(vol, vol),
                    stream_op_printr,
                    stream_op_doer,
                    consume)(diff)
          elif args['diff']:
            diff = tree_diff(vol.tree(), snap)
            pipeline(stream_delta_printr, consume)(diff)
    elif args['remote']:
      if args["add"]:
        remote_vol = getvol(userPath2Path(args['<root>'], cwd))
        vol.remotedb.write(args['<remote>'], remote_vol)
      elif args["remove"]:
        vol.remotedb.delete(args['<remote>'])
      elif args["list"]:
        if args["<remote>"]:
          remote_vol = vol.remotedb.read(args['<remote>'])
          print("\n".join(remote_vol.snapdb.list()))
        else:
          for remote_name in vol.remotedb.list():
            remote_vol = vol.remotedb.read(remote_name)
            print(remote_name, remote_vol.root)
    elif args['pull'] or args['diff']:
      remote_vol = vol.remotedb.read(args['<remote>'])
      snap_name = args['<snap>']
      if snap_name is None:
        remote_snap = remote_vol.tree()
      else:
        remote_snap = remote_vol.snapdb.read(snap_name)
      diff = tree_diff(vol.tree(), remote_snap)
      if args['pull']:
        patcher = tree_patcher(vol, remote_vol)
        pipeline(
                stream_delta_printr,
                patcher,
                stream_op_printr,
                stream_op_doer,
                consume)(diff)
      else: # diff
        pipeline(stream_delta_printr, consume)(diff)
  return exitcode
