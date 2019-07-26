import farmfs
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs.util import empty2dot, fmap, pipeline, concat, identify, uncurry, count, groupby
from farmfs.volume import mkfs, tree_diff, tree_patcher
from os import getcwdu
from fs import Path, userPath2Path
from itertools import ifilter
import sys
from kitchen.text.converters import getwriter
sys.stdout = getwriter('utf8')(sys.stdout)

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

def status(vol, context, path):
  for thawed in vol.thawed(path):
    print thawed.relative_to(context, leading_sep=False)

def op_doer(op):
    (blob_op, tree_op, desc) = op
    blob_op()
    tree_op()

stream_op_doer = fmap(op_doer)

def main():
  args = docopt(USAGE)
  exitcode = 0
  cwd = Path(getcwdu())
  if args['mkfs']:
    root = userPath2Path(args['<root>'] or ".", cwd)
    if args['<data>']:
      data = userPath2Path(args['<data>'], cwd)
    else:
      data = Path(".farmfs/userdata", root)
    mkfs(root, data)
    print "FileSystem Created %s using blobstore %s" % (root, data)
  else:
    vol = getvol(cwd)
    paths = map(lambda x: userPath2Path(x, cwd), empty2dot(args['<path>']))
    def delta_printr(delta):
      deltaPath = delta.path(vol.root).relative_to(cwd, leading_sep=False)
      print "diff: %s %s %s" % (delta.mode, deltaPath, delta.csum)
    stream_delta_printr = fmap(identify(delta_printr))
    def op_printr(op):
      (blob_op, tree_op, (desc, path)) = op
      print desc % path.relative_to(cwd, leading_sep=False)
    stream_op_printr = fmap(identify(op_printr))
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
      pipeline(get_thawed, concat, importer, print_list, list)(paths)
    elif args['thaw']:
      def printr(path):
        print "Exported %s" % path.relative_to(cwd, leading_sep=False)
      exporter = fmap(vol.thaw)
      get_frozen = fmap(vol.frozen)
      print_list = fmap(printr)
      pipeline(get_frozen, concat, exporter, print_list, list)(paths)
    elif args['fsck']:
      # Look for blobs in tree or snaps which are not in blobstore.
      def print_missing_blob(csum, items):
        print "CORRUPTION missing blob %s" % csum
        for item in items:
          props = item.get_dict()
          path = Path(props['path'], vol.root)
          snap = item._snap #TODO touching intenals of item.
          if snap:
            print "\t%s\t%s" % (snap, path.relative_to(cwd, leading_sep=False))
          else:
            print "\t%s"%path.relative_to(cwd, leading_sep=False)
      trees = vol.trees()
      link_checker = vol.link_checker()
      blob_printr = fmap(identify(uncurry(print_missing_blob)))
      missing_blobs = pipeline(
          link_checker,
          blob_printr,
          count)
      bad_blobs = missing_blobs(trees)
      if bad_blobs != 0:
          exitcode = exitcode | 1
      # Look for checksum mismatches.
      def print_checksum_mismatch(csum):
        print "CORRUPTION checksum mismatch in blob %s" % csum #TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
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
      items = vol.trees()
      select_links = partial(ifilter, lambda x: x.is_link())
      group_csums = partial(groupby, lambda item: item.csum())
      def print_count(csum, items):
        print "%s" % csum
        for item in items:
          props = item.get_dict()
          path = Path(props['path'], vol.root)
          snap = props.get('snap', "<tree>")
          print "\t%s\t%s" % (snap, path.relative_to(cwd, leading_sep=False))
      pipeline(
              select_links,
              group_csums,
              fmap(identify(uncurry(print_count))),
              list
              )(items)
    elif args['similarity']:
      for (dir_a, count_a, dir_b, count_b, intersect) in vol.similarity():
        assert isinstance(dir_a, Path)
        assert isinstance(dir_b, Path)
        path_a = dir_a.relative_to(cwd, leading_sep=False)
        path_b = dir_b.relative_to(cwd, leading_sep=False)
        print path_a, "%d/%d %d%%" % (intersect, count_a, int(100*float(intersect)/count_a)), \
                path_b, "%d/%d %d%%" % (intersect, count_b, int(100*float(intersect)/count_b))
    elif args['gc']:
      for f in farmfs.gc(vol):
        print "Removing", f
    elif args['snap']:
      snapdb = vol.snapdb
      if args['list']:
        #TODO have an optional argument for which remote.
        print "\n".join(snapdb.list())
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
              print i
          elif args['restore']:
            tree = vol.tree()
            diff = tree_diff(vol.tree(), snap)
            list(pipeline(
                stream_delta_printr,
                tree_patcher(vol, vol),
                stream_op_printr,
                stream_op_doer)(diff))
          elif args['diff']:
            diff = tree_diff(vol.tree(), snap)
            list(pipeline(stream_delta_printr)(diff))
    elif args['remote']:
      if args["add"]:
        remote_vol = getvol(userPath2Path(args['<root>'], cwd))
        vol.remotedb.write(args['<remote>'], remote_vol)
      elif args["remove"]:
        vol.remotedb.delete(args['<remote>'])
      elif args["list"]:
        if args["<remote>"]:
          remote_vol = vol.remotedb.read(args['<remote>'])
          print "\n".join(remote_vol.snapdb.list())
        else:
          for remote_name in vol.remotedb.list():
            remote_vol = vol.remotedb.read(remote_name)
            print remote_name, remote_vol.root
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
        list(pipeline(
            stream_delta_printr,
            patcher,
            stream_op_printr,
            stream_op_doer)(diff))
      else: # diff
        list(pipeline(stream_delta_printr)(diff))
  exit(exitcode)
