from __future__ import print_function
import farmfs
from farmfs import getvol
from docopt import docopt
from functools import partial
from farmfs import cwd
from farmfs.util import empty2dot, fmap, pipeline, concat, identify, uncurry, count, groupby, consume, concatMap, zipFrom, safetype, ingest, first, maybe, every, identity, repeater, uniq
from farmfs.volume import mkfs, tree_diff, tree_patcher, encode_snapshot, blob_import
from farmfs.fs import Path, userPath2Path, ftype_selector, FILE, LINK, skip_ignored, is_readonly, ensure_symlink
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
      applyfn = fmap(identity) if args.get('--noop') else fmap(vol.delete_blob)
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

from s3lib import Connection as s3conn
from s3lib.ui import load_creds as load_s3_creds
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
      printr = json_printr if args.get('--json') else snapshot_printr
      printr(encode_snapshot(vol.tree()))
    elif args['snap']:
      printr = json_printr if args.get('--json') else snapshot_printr
      printr(encode_snapshot(vol.snapdb.read(args['<snapshot>'])))
    elif args['userdata']:
      printr = json_printr if args.get('--json') else strs_printr
      userdata = pipeline(
              fmap(first),
              fmap(vol.reverser),
              ) (walk([vol.udd], None, [FILE]))
      printr(userdata)
    elif args['keys']:
      printr = json_printr if args.get('--json') else strs_printr
      printr(vol.keydb.list())
  elif args['checksum']:
    #TODO <checksum> <full path>
    paths = imap(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    for p in paths:
      print(p.checksum(), p.relative_to(cwd, leading_sep=False))
  elif args['link']:
    #TODO might move into blobstore.
    f = Path(args['<file>'], cwd)
    b = ingest(args['<target>'])
    bp = vol.csum_to_path(b)
    if not bp.exists():
      print("blob %s doesn't exist" % b)
      if args['--remote']:
        remote = vol.remotedb.read(args['--remote'])
      else:
        raise(ValueError("aborting due to missing blob"))
      rbp = remote.csum_to_path(b)
      blob_import(rbp, bp)
    else:
      pass #bp exists, can we check its checksum?
    ensure_symlink(f, bp)
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
    snapNames = args['<snap>']
    def missing_printr(csum, pathStrs):
        paths = sorted(imap(lambda pathStr: vol.root.join(pathStr), pathStrs))
        for path in paths:
            print("%s\t%s" % (csum, path.relative_to(cwd, leading_sep=False)))
    missing_csum2pathStr = pipeline(
            fmap(vol.snapdb.read),
            concatMap(iter),
            partial(ifilter, lambda item: item.is_link()),
            partial(ifilter, lambda item: not vol.is_ignored(item.to_path(vol.root), None)),
            partial(ifilter, lambda item: item.csum() not in tree_csums),
            partial(groupby, lambda item: item.csum()),
            partial(ifilter, uncurry(lambda csum, items: every(lambda item: not item.to_path(vol.root).exists(), items))),
            fmap(uncurry(lambda csum, items: (csum, list(imap(lambda item: item.pathStr(), items))))),
            fmap(uncurry(missing_printr)),
            count
            )(snapNames)
  elif args['blobtype']:
    for blob in args['<blob>']:
      blob = ingest(blob)
      print(
              blob,
              maybe("unknown", vol.csum_to_path(blob).filetype()))
  elif args['blob']:
    for csum in args['<blob>']:
      csum = ingest(csum)
      print(csum,
              vol.csum_to_path(csum).relative_to(cwd, leading_sep=False))
  elif args['s3']:
      bucket = args['<bucket>']
      prefix = args['<prefix>'] + "/"
      access_id, secret_key = load_s3_creds(None)
      with s3conn(access_id, secret_key) as s3:
          key_iter = s3.list_bucket(bucket, prefix=prefix)
          if args['list']:
              pipeline(fmap(print), consume)(key_iter)
          elif args['upload']:
              keys = set(key_iter)
              tree = vol.tree()
              def upload(csum):
                  blob = vol.csum_to_path(csum)
                  key = prefix + csum
                  print(csum, "->", blob, "->", key)
                  with blob.open('rb') as f:
                      #TODO should provide pre-calculated md5 rather than recompute.
                      result = s3.put_object(bucket, key, f.read())
                  return result
              http_success = lambda status_headers: status_headers[0] >=200 and status_headers[0] < 300
              s3_exception = lambda e: isinstance(e, ValueError)
              upload_repeater = repeater(upload, max_tries = 3, predicate = http_success, catch_predicate = s3_exception)
              pipeline(
                      partial(ifilter, lambda x: x.is_link()),
                      fmap(lambda x: x.csum()),
                      partial(ifilter, lambda x: x not in keys),
                      uniq,
                      fmap(upload_repeater),
                      consume
                      )(iter(tree))
  return exitcode
