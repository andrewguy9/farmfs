from __future__ import print_function
from farmfs import getvol
from docopt import docopt
from farmfs import cwd
from shutil import copyfileobj
from farmfs.util import \
    concat,        \
    concatMap,     \
    consume,       \
    count,         \
    empty_default, \
    every,         \
    ffilter,       \
    first,         \
    finvert,       \
    fmap,          \
    groupby,       \
    identify,      \
    identity,      \
    ingest,        \
    maybe,         \
    partial,       \
    pfmap,         \
    pipeline,      \
    safetype,      \
    uncurry,       \
    uniq,          \
    zipFrom
from farmfs.volume import mkfs, tree_diff, tree_patcher, encode_snapshot
from farmfs.fs import Path, userPath2Path, ftype_selector, LINK, skip_ignored, walk
from json import JSONEncoder
from s3lib.ui import load_creds as load_s3_creds
import sys
from farmfs.blobstore import S3Blobstore
from tqdm import tqdm
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
if sys.version_info >= (3, 0):
    def getBytesStdOut():
        "On python 3+, sys.stdout.buffer is bytes writable."
        return sys.stdout.buffer
else:
    def getBytesStdOut():
        "On python 2, sys.stdout is bytes writable."
        return sys.stdout

json_encoder = JSONEncoder(ensure_ascii=False, sort_keys=True)
json_encode = lambda data: json_encoder.encode(data)
json_printr = pipeline(list, json_encode, print)
strs_printr = pipeline(fmap(print), consume)

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


debug = fmap(identify(eprint))

def dict_printr(keys, d):
    print("\t".join([ingest(d.get(k, '')) for k in keys]))

def dicts_printr(keys):
    return pipeline(fmap(partial(dict_printr, keys)), consume)


snapshot_printr = dicts_printr(['path', 'type', 'csum'])

UI_USAGE = """
FarmFS

Usage:
  farmfs mkfs [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap list
  farmfs snap (make|read|delete|restore|diff) <snap>
  farmfs fsck [--broken --frozen-ignored --blob-permissions --checksums]
  farmfs count
  farmfs similarity <dir_a> <dir_b>
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
    tree_items = concatMap(lambda t: zipFrom(t, iter(t)))
    tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
    broken_tree_links = partial(
        ifilter,
        uncurry(lambda snap, item: not vol.bs.exists(item.csum())))
    checksum_grouper = partial(groupby,
                               uncurry(lambda snap, item: item.csum()))
    def broken_link_printr(csum, snap_items):
        print(csum)
        for (snap, item) in snap_items:
            print('',
                  snap.name,
                  item.to_path(vol.root).relative_to(cwd),
                  sep='\t')
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
    # TODO some of this logic could be moved to volume. Which files are members of the volume is a function of the volume.
    ignore_mdd = partial(skip_ignored, [safetype(vol.mdd)])
    ignored_frozen = pipeline(
        ftype_selector([LINK]),
        ffilter(uncurry(vol.is_ignored)),
        fmap(first),
        fmap(lambda p: p.relative_to(cwd)),
        fmap(partial(print, "Ignored file frozen")),
        count
    )(walk(vol.root, skip=ignore_mdd))
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
    # TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
    mismatches = pipeline(
        pfmap(lambda blob: (blob, vol.bs.blob_checksum(blob))),
        ffilter(lambda blob_csum: blob_csum[0] != blob_csum[1]),
        fmap(lambda blob_csum: print("CORRUPTION checksum mismatch in blob %s got %s" % (blob_csum[0], blob_csum[1]))),
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
        paths = empty_default(map(lambda x: userPath2Path(x, cwd), args['<path>']), [vol.root])
        def delta_printr(delta):
            deltaPath = delta.path(vol.root).relative_to(cwd)
            print("diff: %s %s %s" % (delta.mode, deltaPath, delta.csum))
        stream_delta_printr = fmap(identify(delta_printr))
        def op_printr(op):
            (blob_op, tree_op, (desc, path)) = op
            print(desc % path.relative_to(cwd))
        stream_op_printr = fmap(identify(op_printr))
        if args['status']:
            get_thawed = fmap(vol.thawed)
            pipeline(
                get_thawed,
                concat,
                fmap(lambda p: p.relative_to(cwd)),
                fmap(print),
                consume
            )(paths)
        elif args['freeze']:
            def printr(freeze_op):
                s = "Imported %s with checksum %s" % \
                    (freeze_op['path'].relative_to(cwd),
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
                print("Exported %s" % path.relative_to(cwd))
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
            tree_items = concatMap(lambda t: zipFrom(t, iter(t)))
            tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
            checksum_grouper = partial(
                groupby,
                uncurry(lambda snap, item: item.csum()))
            def count_printr(csum, snap_items):
                print(csum, count(snap_items))
                for (snap, item) in snap_items:
                    print(snap.name, item.to_path(vol.root).relative_to(cwd))
            counts_printr = fmap(identify(uncurry(count_printr)))
            pipeline(
                tree_items,
                tree_links,
                checksum_grouper,
                counts_printr,
                consume
            )(trees)
        elif args['similarity']:
            dir_a = userPath2Path(args['<dir_a>'], cwd)
            dir_b = userPath2Path(args['<dir_b>'], cwd)
            print("left", "both", "right", "jaccard_similarity", sep="\t")
            print(* vol.similarity(dir_a, dir_b), sep="\t")
        elif args['gc']:
            applyfn = fmap(identity) if args.get('--noop') else fmap(vol.bs.delete_blob)
            fns = [
                fmap(identify(partial(print, "Removing"))),
                applyfn,
                consume
            ]
            pipeline(*fns)(sorted(vol.unused_blobs(vol.items())))
        elif args['snap']:
            snapdb = vol.snapdb
            if args['list']:
                # TODO have an optional argument for which remote.
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
                        diff = tree_diff(vol.tree(), snap)
                        pipeline(
                            stream_delta_printr,
                            tree_patcher(vol, vol),
                            stream_op_printr,
                            stream_op_doer,
                            consume
                        )(diff)
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
                    consume
                )(diff)
            else:  # diff
                pipeline(stream_delta_printr, consume)(diff)
    return exitcode


def printNotNone(value):
    if value is not None:
        print(value)


DBG_USAGE = \
    """
    FarmDBG

    Usage:
      farmdbg reverse [--snap=<snapshot>|--all] <csum>...
      farmdbg key read <key>
      farmdbg key write <key> <value>
      farmdbg key delete <key>
      farmdbg key list [<key>]
      farmdbg walk (keys|userdata|root|snap <snapshot>) [--json]
      farmdbg checksum <path>...
      farmdbg fix link [--remote=<remote>] <target> <file>
      farmdbg rewrite-links
      farmdbg missing <snap>...
      farmdbg blobtype <blob>...
      farmdbg blob path <blob>...
      farmdbg blob read <blob>...
      farmdbg s3 list <bucket> <prefix>
      farmdbg s3 upload (local|all|snap <snapshot>) [--quiet] <bucket> <prefix>
      farmdbg s3 check <bucket> <prefix>
      farmdbg s3 read <bucket> <prefix> <blob>...
      farmdbg redact pattern [--noop] <pattern> <from>
    """

def dbg_main():
    return dbg_ui(sys.argv[1:], cwd)

def dbg_ui(argv, cwd):
    exitcode = 0
    args = docopt(DBG_USAGE, argv)
    vol = getvol(cwd)
    if args['reverse']:
        csums = args['<csum>']
        if args['--all']:
            trees = vol.trees()
        elif args['--snap']:
            trees = [vol.snapdb.read(args['--snap'])]
        else:
            trees = [vol.tree()]
        tree_items = concatMap(lambda t: zipFrom(t, iter(t)))
        tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
        matching_links = ffilter(uncurry(lambda snap, item: item.csum() in csums))
        def link_printr(snap_item):
            (snap, item) = snap_item
            print(item.csum(), snap.name, item.to_path(vol.root).relative_to(cwd))
        links_printr = fmap(identify(link_printr))
        pipeline(
            tree_items,
            tree_links,
            matching_links,
            links_printr,
            consume
        )(trees)
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
            # TODO could add a test for output encoding.
            # TODO could add a test for snap format. Leading '/' on paths.
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
        # TODO <checksum> <full path>
        paths = empty_default(map(lambda x: Path(x, cwd), args['<path>']), [vol.root])
        for p in paths:
            print(p.checksum(), p.relative_to(cwd))
    elif args['link']:
        f = Path(args['<file>'], cwd)
        b = ingest(args['<target>'])
        if not vol.bs.exists(b):
            print("blob %s doesn't exist" % b)
            if args['--remote']:
                remote = vol.remotedb.read(args['--remote'])
            else:
                raise ValueError("aborting due to missing blob")
            vol.bs.fetch_blob(remote.bs, b)
        else:
            pass  # b exists, can we check its checksum?
        vol.bs.link_to_blob(f, b)
    elif args['rewrite-links']:
        for item in vol.tree():
            if not item.is_link():
                continue
            path = item.to_path(vol.root)
            new = vol.repair_link(path)
            if new is not None:
                print("Relinked %s to %s" % (path.relative_to(cwd), new))
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
                print("%s\t%s" % (csum, path.relative_to(cwd)))
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
        if missing_csum2pathStr > 0:
            exitcode = exitcode | 4
    elif args['blobtype']:
        for blob in args['<blob>']:
            blob = ingest(blob)
            # TODO here csum_to_path is really needed.
            print(
                blob,
                maybe("unknown", vol.bs.csum_to_path(blob).filetype()))
    elif args['blob']:
        if args['path']:
            for csum in args['<blob>']:
                csum = ingest(csum)
                # TODO here csum_to_path is needed
                print(csum, vol.bs.csum_to_path(csum).relative_to(cwd))
        elif args['read']:
            for csum in args['<blob>']:
                with vol.bs.read_handle(csum) as fd:
                    copyfileobj(fd, getBytesStdOut())
    elif args['s3']:
        bucket = args['<bucket>']
        prefix = args['<prefix>']
        access_id, secret_key = load_s3_creds(None)
        s3bs = S3Blobstore(bucket, prefix, access_id, secret_key)
        if args['list']:
            pipeline(fmap(print), consume)(s3bs.blobs()())
        elif args['upload']:
            quiet = args.get('--quiet')
            print("Calculating remote blobs")
            s3_blobs = set(tqdm(s3bs.blobs()(), disable=quiet, desc="Calculating remote blobs", smoothing=1.0, dynamic_ncols=True, maxinterval=1.0))
            print("Remote Blobs: %s" % len(s3_blobs))
            print("Calculating local blobs")  # TODO we are looking at tree, so blobs in snaps won't be sent.
            if args.get('local'):
                src_pipe = pipeline(
                    ffilter(lambda x: x.is_link()),
                    fmap(lambda x: x.csum()),
                    uniq,
                )(iter(vol.tree()))
            elif args.get('all'):
                src_pipe = vol.bs.blobs()
            elif args.get('snap'):
                snap_name = args.get('<snapshot>')
                src_pipe = pipeline(
                    ffilter(lambda x: x.is_link()),
                    fmap(lambda x: x.csum()),
                    uniq,
                )(iter(vol.snapdb.read(snap_name)))
            else:
                raise ValueError("Invalid upload case", args)
            src_blobs = set(tqdm(src_pipe, disable=quiet, desc="Calculating local blobs", smoothing=1.0, dynamic_ncols=True, maxinterval=1.0))
            print("Local Blobs: %s" % len(src_blobs))
            upload_blobs = src_blobs - s3_blobs
            print("Uploading %s blobs to s3" % len(upload_blobs))
            with tqdm(desc="Uploading to S3", disable=quiet, total=len(upload_blobs), smoothing=1.0, dynamic_ncols=True, maxinterval=1.0) as pbar:
                def update_pbar(blob):
                    pbar.update(1)
                    pbar.set_description("Uploaded %s" % blob)
                def upload(blob):
                    s3bs.upload(blob, vol.bs.csum_to_path(blob))()
                    return blob
                all_success = pipeline(
                    ffilter(lambda x: x not in s3_blobs),
                    pfmap(upload, workers=2),
                    fmap(identify(update_pbar)),
                    partial(every, identity),
                )(upload_blobs)
                if all_success:
                    print("Successfully uploaded")
                else:
                    print("Failed to upload")
                    exitcode = exitcode | 1
        elif args['check']:
            num_corrupt_blobs = pipeline(
                ffilter(lambda obj: obj['ETag'][1:-1] != obj['blob']),
                fmap(identify(lambda obj: print(obj['blob'], obj['ETag'][1:-1]))),
                count
            )(s3bs.blob_stats()())
            if num_corrupt_blobs == 0:
                print("All S3 blobs etags match")
            else:
                exitcode = exitcode | 2
        elif args['read']:
            for blob in args.get('<blob>'):
                with s3bs.read_handle(blob) as fd:
                    copyfileobj(fd, getBytesStdOut())
    elif args['redact']:
        pattern = args['<pattern>']
        ignored = [pattern]
        snapName = args['<from>']
        snap = vol.snapdb.read(snapName)
        is_redacted = partial(skip_ignored, ignored)
        printr = lambda item: print("redacted", item.to_path(vol.root).relative_to(cwd))
        def show_redacted(item):
            if is_redacted(item):
                printr(item)
            return item
        is_kept = finvert(is_redacted)
        out_snap = pipeline(
            fmap(show_redacted),
            ffilter(is_kept)
        )(iter(snap))
        if args['--noop']:
            consume(out_snap)
        else:
            vol.snapdb.write(snapName, out_snap)
    return exitcode
