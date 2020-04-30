from __future__ import print_function
import farmfs
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs import cwd
from farmfs.util import empty2dot, fmap, pipeline, concat, identify, uncurry, count, groupby, consume, concatMap, zipFrom, safetype, ingest, first, maybe
from farmfs.volume import mkfs, tree_diff, tree_patcher, encode_snapshot
from farmfs.fs import Path, userPath2Path, ftype_selector, FILE, LINK, skip_ignored, is_readonly
from json import JSONEncoder
import sys
try:
    from itertools import ifilter
except ImportError:
    # On python3, filter is lazy.
    ifilter = filter
try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map

UI_USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap list
  farmfs snap (make|read|delete|restore|diff) <snap>
  farmfs fsck [--broken --frozen-ignored --blob-permissions --checksums]
  farmfs count
  farmfs similarity
  farmfs gc [--noop]
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


def fsck_missing_blobs(vol, cwd):
    '''Look for blobs in tree or snaps which are not in blobstore.'''
    trees = vol.trees()
    tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
    tree_links = partial(ifilter, uncurry(lambda snap, item: item.is_link()))
    broken_tree_links = partial(
            ifilter,
            uncurry(lambda snap, item: not vol.blob_checker(item.csum())))
    checksum_grouper = partial(groupby,
            uncurry(lambda snap, item: item.csum()))
    def broken_link_printr(csum, snap_items):
        print(csum)
        for (snap, item) in snap_items:
            print(
                    "\t",
                    snap.name,
                    item.to_path(vol.root).relative_to(cwd, leading_sep=False))
    broken_links_printr = fmap(identify(uncurry(broken_link_printr)))
    num_bad_blobs = pipeline(
            tree_items,
            tree_links,
            broken_tree_links,
            checksum_grouper,
            broken_links_printr,
            count)(trees)
    return num_bad_blobs

def fsck_frozen_ignored(vol, cwd):
    '''Look for frozen links which are in the ignored file.'''
    ignore_mdd = partial(skip_ignored, [safetype(vol.mdd)])
    ignored_frozen = pipeline(
            ftype_selector([LINK]),
            partial(ifilter, uncurry(vol.is_ignored)),
            fmap(first),
            fmap(lambda p: p.relative_to(cwd, leading_sep=False)),
            fmap(partial(print, "Ignored file frozen")),
            count
            )(vol.root.entries(ignore_mdd))
    return ignored_frozen

def fsck_blob_permissions(vol, cwd):
    '''Look for blobstore blobs which are not readonly.'''
    blob_permissions = pipeline(
            partial(ifilter, is_readonly),
            fmap(vol.reverser),
            fmap(partial(print, "writable blob: ")),
            count
            )(vol.userdata_files())
    return blob_permissions

def fsck_checksum_mismatches(vol, cwd):
    '''Look for checksum mismatches.'''
    select_broken = partial(ifilter, vol.check_userdata_blob)
    #TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
    mismatches = pipeline(
            select_broken,
            fmap(vol.reverser),
            fmap(lambda csum: print("CORRUPTION checksum mismatch in blob %s" % csum)),
            count
            )(vol.userdata_files())
    return mismatches

def ui_main():
    result = farmfs_ui(sys.argv[1:], cwd)
    exit(result)

def farmfs_ui(argv, cwd):
  exitcode = 0
  args = docopt(UI_USAGE, argv)
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
        fsck_actions = {
                '--broken': (fsck_missing_blobs, 1),
                '--frozen-ignored': (fsck_frozen_ignored, 4),
                '--blob-permissions': (fsck_blob_permissions, 8),
                '--checksums': (fsck_checksum_mismatches, 2),
                }
        fsck_tasks = [action for (verb, action) in fsck_actions.items() if args[verb]]
        if len(fsck_tasks) == 0:
            # No options were specified, run the whole sweet.
            fsck_tasks = fsck_actions.values()
        for foo, fail_code in fsck_tasks:
            exitcode = exitcode | (foo(vol, cwd) and fail_code)
    elif args['count']:
      trees = vol.trees()
      tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
      tree_links = partial(ifilter, uncurry(lambda snap, item: item.is_link()))
      checksum_grouper = partial(groupby,
              uncurry(lambda snap, item: item.csum()))
      def count_printr(csum, snap_items):
        print(csum, count(snap_items))
        for (snap, item) in snap_items:
            print(snap.name, item.to_path(vol.root).relative_to(cwd, leading_sep=False))
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
      if args['--noop']:
        fns = [fmap(identify(partial(print, "Removing"))),
                consume]
      else:
        fns = [fmap(identify(partial(print, "Removing"))),
                fmap(vol.delete_blob),
                consume]
      pipeline(*fns)(sorted(vol.unused_blobs(vol.items())))
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


def printNotNone(value):
  if value is not None:
    print(value)

def walk(parents, is_ignored, match):
  return pipeline(
          concatMap(lambda parent: parent.entries(is_ignored)),
          ftype_selector(match)
          )(iter(parents))

def reverse(vol, csum):
  """Yields a set of paths which reference a given checksum_path name."""

DBG_USAGE = \
"""
FarmDBG

Usage:
  farmdbg reverse <csum>
  farmdbg key read <key>
  farmdbg key write <key> <value>
  farmdbg key delete <key>
  farmdbg key list [<key>]
  farmdbg walk (keys|userdata|root|snap <snapshot>)
  farmdbg checksum <path>...
  farmdbg fix link <file> <target>
  farmdbg rewrite-links <target>
  farmdbg missing <snap>
  farmdbg blobtype <blob>...
"""

def dbg_main():
  return dbg_ui(sys.argv[1:], cwd)

def dbg_ui(argv, cwd):
  exitcode = 0
  args = docopt(DBG_USAGE, argv)
  vol = getvol(cwd)
  if args['reverse']:
    csum = args['<csum>']
    trees = vol.trees()
    tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
    tree_links = partial(ifilter, uncurry(lambda snap, item: item.is_link()))
    matching_links = partial(ifilter, uncurry(lambda snap, item: item.csum() == csum))
    def link_printr(snap_item):
        (snap, item) = snap_item
        print(snap.name, item.to_path(vol.root).relative_to(cwd, leading_sep=False))
    links_printr = fmap(identify(link_printr))
    pipeline(
            tree_items,
            tree_links,
            matching_links,
            links_printr,
            consume)(trees)
  elif args['key']:
    db = vol.keydb
    key = args['<key>']
    if args['read']:
      printNotNone(db.readraw(key))
    elif args['delete']:
      db.delete(key)
    elif args['list']:
      for v in db.list(key):
        print(v)
    elif args['write']:
      value = args['<value>']
      db.write(key, value)
  elif args['walk']:
    if args['root']:
      print(JSONEncoder(ensure_ascii=False, sort_keys=True).encode(encode_snapshot(vol.tree())))
    elif args['snap']:
      print(JSONEncoder(ensure_ascii=False, sort_keys=True).encode(encode_snapshot(vol.snapdb.read(args['<snapshot>']))))
    elif args['userdata']:
      userdata = pipeline(
              fmap(first),
              fmap(vol.reverser),
              list
              ) (walk([vol.udd], None, [FILE]))
      print(JSONEncoder(ensure_ascii=False, sort_keys=True).encode(userdata))
    elif args['keys']:
      print(JSONEncoder(ensure_ascii=False, sort_keys=True).encode(vol.keydb.list()))
  elif args['checksum']:
    #TODO <checksum> <full path>
    paths = imap(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    for p in paths:
      print(p.checksum(), p.relative_to(cwd, leading_sep=False))
  elif args['link']:
    f = Path(args['<file>'], cwd)
    t = Path(args['<target>'], cwd)
    if not f.islink():
      raise ValueError("%s is not a link. Refusing to fix" % (f))
    f.unlink()
    f.symlink(t)
  elif args['rewrite-links']:
    target = Path(args['<target>'], cwd)
    for (link, _type) in walk([target], [safetype(vol.mdd)], [LINK]):
      new = vol.repair_link(link)
      if new is not None:
          print("Relinked %s to %s" % (link.relative_to(cwd, leading_sep=False), new))
  elif args['missing']:
    tree_csums = pipeline(
            partial(ifilter, lambda item: item.is_link()),
            fmap(lambda item: item.csum()),
            set
            )(iter(vol.tree()))
    snapName = args['<snap>']
    snap = vol.snapdb.read(snapName)
    def missing_printr(csum, pathStrs):
        print("Missing csum %s with paths:" % csum)
        paths = sorted(imap(lambda pathStr: vol.root.join(pathStr), pathStrs))
        for path in paths:
            print("\t%s" % path.relative_to(cwd, leading_sep=False))
    missing_csum2pathStr = pipeline(
            partial(ifilter, lambda item: item.is_link()),
            partial(ifilter, lambda item: not vol.is_ignored(item.to_path(vol.root), None)),
            partial(ifilter, lambda item: item.csum() not in tree_csums),
            partial(groupby, lambda item: item.csum()),
            fmap(uncurry(lambda csum, items: (csum, list(imap(lambda item: item.pathStr(), items))))),
            fmap(uncurry(missing_printr)),
            count
            )(iter(snap))
  elif args['blobtype']:
    for blob in args['<blob>']:
      blob = ingest(blob)
      print(
              blob,
              maybe("unknown", vol.csum_to_path(blob).filetype()))
  return exitcode
