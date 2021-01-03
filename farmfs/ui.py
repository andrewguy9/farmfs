from __future__ import print_function
import farmfs
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs import cwd
from farmfs.util import empty2dot, fmap, ffilter, pipeline, concat, identify, uncurry, count, groupby, consume, concatMap, zipFrom, safetype, ingest, first, maybe, every, identity, repeater, uniq, compose
from farmfs.volume import mkfs, tree_diff, tree_patcher, encode_snapshot
from farmfs.fs import Path, userPath2Path, ftype_selector, FILE, LINK, skip_ignored, ensure_symlink
from json import JSONEncoder
from s3lib.ui import load_creds as load_s3_creds
import sys
from farmfs.blobstore import S3Blobstore
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

json_encode = lambda data: JSONEncoder(ensure_ascii=False, sort_keys=True).encode(data)
json_printr = pipeline(list, json_encode, print)
strs_printr = pipeline(fmap(print), consume)

def dict_printr(keys, d):
    print("\t".join([ingest(d.get(k, '')) for k in keys]))

def dicts_printr(keys):
    return pipeline(fmap(partial(dict_printr, keys)), consume)

snapshot_printr = dicts_printr(['path', 'type', 'csum'])

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
    tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
    broken_tree_links = partial(
            ifilter,
            uncurry(lambda snap, item: not vol.bs.exists(item.csum())))
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
    #TODO some of this logic could be moved to volume. Which files are members of the volume is a function of the volume.
    ignore_mdd = partial(skip_ignored, [safetype(vol.mdd)])
    ignored_frozen = pipeline(
            ftype_selector([LINK]),
            ffilter(uncurry(vol.is_ignored)),
            fmap(first),
            fmap(lambda p: p.relative_to(cwd, leading_sep=False)),
            fmap(partial(print, "Ignored file frozen")),
            count
            )(vol.root.entries(ignore_mdd))
    return ignored_frozen

def fsck_blob_permissions(vol, cwd):
    '''Look for blobstore blobs which are not readonly.'''
    blob_permissions = pipeline(
            ffilter(vol.bs.verify_blob_permissions),
            fmap(partial(print, "writable blob: ")),
            count
            )(vol.bs.blobs())
    return blob_permissions

def fsck_checksum_mismatches(vol, cwd):
    '''Look for checksum mismatches.'''
    #TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
    mismatches = pipeline(
            ffilter(vol.bs.verify_blob_checksum),
            fmap(lambda csum: print("CORRUPTION checksum mismatch in blob %s" % csum)),
            count
            )(vol.bs.blobs())
    return mismatches

def ui_main():
    result = farmfs_ui(sys.argv[1:], cwd)
    exit(result)

def farmfs_ui(argv, cwd):
  exitcode = 0
  args = docopt(UI_USAGE, argv)
  if args['mkfs']:
    root = userPath2Path(args['<root>'] or ".", cwd)
    data = userPath2Path(args['<data>'], cwd) if args.get('<data>') else Path(".farmfs/userdata", root)
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
            # No options were specified, run the whole suite.
            fsck_tasks = fsck_actions.values()
        for foo, fail_code in fsck_tasks:
            exitcode = exitcode | (foo(vol, cwd) and fail_code)
    elif args['count']:
      trees = vol.trees()
      tree_items = concatMap(lambda t: zipFrom(t,iter(t)))
      tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
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
      applyfn = fmap(identity) if args.get('--noop') else fmap(vol.bs.delete_blob)
      fns = [fmap(identify(partial(print, "Removing"))),
              applyfn,
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
      remote_snap = remote_vol.snapdb.read(snap_name) if snap_name else remote_vol.tree()
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
  farmdbg walk (keys|userdata|root|snap <snapshot>) [--json]
  farmdbg checksum <path>...
  farmdbg fix link [--remote=<remote>] <target> <file>
  farmdbg rewrite-links <target>
  farmdbg missing <snap>...
  farmdbg blobtype <blob>...
  farmdbg blob <blob>...
  farmdbg s3 list <bucket> <prefix>
  farmdbg s3 upload <bucket> <prefix>
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
    tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
    matching_links = ffilter(uncurry(lambda snap, item: item.csum() == csum))
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
      printr = json_printr if args.get('--json') else snapshot_printr
      printr(encode_snapshot(vol.tree()))
    elif args['snap']:
      printr = json_printr if args.get('--json') else snapshot_printr
      printr(encode_snapshot(vol.snapdb.read(args['<snapshot>'])))
    elif args['userdata']:
      blobs = vol.bs.blobs()
      printr = json_printr if args.get('--json') else strs_printr
      printr(blobs)
    elif args['keys']:
      printr = json_printr if args.get('--json') else strs_printr
      printr(vol.keydb.list())
  elif args['checksum']:
    #TODO <checksum> <full path>
    paths = imap(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    for p in paths:
      print(p.checksum(), p.relative_to(cwd, leading_sep=False))
  elif args['link']:
    f = Path(args['<file>'], cwd)
    b = ingest(args['<target>'])
    if not vol.bs.exists(b):
      print("blob %s doesn't exist" % b)
      if args['--remote']:
        remote = vol.remotedb.read(args['--remote'])
      else:
        raise(ValueError("aborting due to missing blob"))
      vol.bs.fetch_blob(remote.bs, b)
    else:
      pass #b exists, can we check its checksum?
    vol.bs.link_to_blob(f, b)
  elif args['rewrite-links']:
    target = Path(args['<target>'], cwd)
    for item in vol.tree():
        if not item.is_link():
            continue
        path = item.to_path(vol.root)
        new = vol.repair_link(path)
        if new is not None:
            print("Relinked %s to %s" % (path.relative_to(cwd, leading_sep=False), new))
  elif args['missing']:
    tree_csums = pipeline(
            ffilter(lambda item: item.is_link()),
            fmap(lambda item: item.csum()),
            set
            )(iter(vol.tree()))
    snapNames = args['<snap>']
    def missing_printr(csum, pathStrs):
        paths = sorted(imap(lambda pathStr: vol.root.join(pathStr), pathStrs))
        for path in paths:
            print("%s\t%s" % (csum, path.relative_to(cwd, leading_sep=False)))
    missing_csum2pathStr = pipeline(
            fmap(vol.snapdb.read),
            concatMap(iter),
            ffilter(lambda item: item.is_link()),
            ffilter(lambda item: not vol.is_ignored(item.to_path(vol.root), None)),
            ffilter(lambda item: item.csum() not in tree_csums),
            partial(groupby, lambda item: item.csum()),
            ffilter(uncurry(lambda csum, items: every(lambda item: not item.to_path(vol.root).exists(), items))),
            fmap(uncurry(lambda csum, items: (csum, list(imap(lambda item: item.pathStr(), items))))),
            fmap(uncurry(missing_printr)),
            count
            )(snapNames)
  elif args['blobtype']:
    for blob in args['<blob>']:
      blob = ingest(blob)
      #TODO here csum_to_path is really needed.
      print(
              blob,
              maybe("unknown", vol.bs.csum_to_path(blob).filetype()))
  elif args['blob']:
    for csum in args['<blob>']:
      csum = ingest(csum)
      #TODO here csum_to_path is needed
      print(csum,
              vol.bs.csum_to_path(csum).relative_to(cwd, leading_sep=False))
  elif args['s3']:
      bucket = args['<bucket>']
      prefix = args['<prefix>']
      access_id, secret_key = load_s3_creds(None)
      s3bs = S3Blobstore(bucket, prefix, access_id, secret_key)
      blobs = s3bs.blobs()
      if args['list']:
          pipeline(fmap(print), consume)(blobs())
      elif args['upload']:
          keys = set(blobs())
          print("Cached %s keys" % len(keys))
          if len(keys) > 0:
              print("Cached key example", list(keys)[0])
          tree = vol.tree()
          all_success = pipeline(
                  ffilter(lambda x: x.is_link()),
                  fmap(lambda x: x.csum()),
                  fmap(identify(partial(print, "checking key"))),
                  ffilter(lambda x: x not in keys),
                  fmap(identify(partial(print, "uploading key"))),
                  uniq,
                  fmap(lambda blob: s3bs.upload(blob, vol.bs.csum_to_path(blob))),
                  fmap(lambda downloader: downloader()),
                  partial(every, identity),
                  )(iter(tree))
          if all_success:
              print("Successfully uploaded")
          else:
              print("Failed to upload")
              exitcode = 1
  return exitcode
