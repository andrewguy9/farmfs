from __future__ import print_function
from farmfs import getvol
from docopt import docopt
from farmfs import cwd
from farmfs.util import \
    concat,        \
    concatMap,     \
    consume,       \
    count,         \
    copyfileobj,   \
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
    pfmaplazy,     \
    pipeline,      \
    safetype,      \
    uncurry,       \
    uniq,          \
    zipFrom
from farmfs.volume import mkfs, tree_diff, tree_patcher, encode_snapshot
from farmfs.fs import Path, userPath2Path, ftype_selector, LINK, skip_ignored, walk, ensure_symlink
from json import JSONEncoder
from s3lib.ui import load_creds as load_s3_creds
import sys
from farmfs.blobstore import S3Blobstore, HttpBlobstore
from tqdm import tqdm
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
  farmfs snap (make|read|delete|restore|diff) [--force] <snap>
  farmfs fsck [--missing --frozen-ignored --blob-permissions --checksums] [--fix]
  farmfs count
  farmfs similarity <dir_a> <dir_b>
  farmfs gc [--noop]
  farmfs remote add [--force] <remote> <root>
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

def fsck_fix_missing_blobs(vol, remote):
    bs = vol.bs
    select_csum = first
    if remote is None:
        raise ValueError("No remote specified, cannot restore missing blobs")
    def download_missing_blob(csum):
        getSrcHandleFn = lambda: remote.bs.read_handle(csum)
        vol.bs.import_via_fd(getSrcHandleFn, csum)
        return csum
    printr = fmap(lambda csum: print("\tRestored ", csum, "from remote"))
    return pipeline(fmap(select_csum), fmap(download_missing_blob), printr)

def fsck_missing_blobs(vol, cwd):
    '''Look for blobs in tree or snaps which are not in blobstore.'''
    trees = vol.trees()
    tree_items = concatMap(lambda t: zipFrom(t, iter(t)))
    tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
    broken_tree_links = partial(
        filter,
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
    bad_blobs = pipeline(
        tree_items,
        tree_links,
        broken_tree_links,
        checksum_grouper,
        broken_links_printr)(trees)
    return bad_blobs

def fsck_fix_frozen_ignored(vol, remote):
    '''Thaw out files in the tree which are ignored.'''
    fixer = fmap(vol.thaw)
    printr = fmap(lambda p: print("Thawed", p.relative_to(vol.root)))
    return pipeline(fixer, printr)

def fsck_frozen_ignored(vol, cwd):
    '''Look for frozen links which are in the ignored file.'''
    # TODO some of this logic could be moved to volume. Which files are members of the volume is a function of the volume.
    ignore_mdd = partial(skip_ignored, [safetype(vol.mdd)])
    ignored_frozen = pipeline(
        ftype_selector([LINK]),
        ffilter(uncurry(vol.is_ignored)),
        fmap(first),
        fmap(identify(lambda p: print("Ignored file frozen", p.relative_to(cwd))))
    )(walk(vol.root, skip=ignore_mdd))
    return ignored_frozen

def fsck_fix_blob_permissions(vol, remote):
    fixer = fmap(identify(vol.bs.fix_blob_permissions))
    printr = fmap(lambda blob: print("fixed blob permissions:", blob))
    return pipeline(fixer, printr)

def fsck_blob_permissions(vol, cwd):
    '''Look for blobstore blobs which are not readonly, and fix them.'''
    blob_permissions = pipeline(
        ffilter(finvert(vol.bs.verify_blob_permissions)),
        fmap(identify(partial(print, "writable blob: ")))
    )(vol.bs.blobs())
    return blob_permissions

# TODO if the corruption fix fails, we don't fail the command.
def fsck_fix_checksum_mismatches(vol, remote):
    if remote is None:
        raise ValueError("No remote specified, cannot restore missing blobs")
    def checksum_fixer(blob):
        remote_csum = remote.bs.blob_checksum(blob)
        if remote_csum == blob:
            getSrcHandleFn = lambda: remote.bs.read_handle(blob)
            # TODO will be a duplicate, so we need a way to force the re-import/replacement.
            vol.bs.import_via_fd(getSrcHandleFn, blob, force=True)
            print("REPLICATED blob %s from remote" % blob)
        else:
            print("Cannot copy blob %s, remote blob also has mismatched checksum", blob)
    fixer = identify(checksum_fixer)
    return pipeline(fmap(fixer))

def fsck_checksum_mismatches(vol, cwd):
    '''Look for checksum mismatches.'''
    # TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
    mismatches = pipeline(
        pfmaplazy(lambda blob: (blob, vol.bs.blob_checksum(blob))),
        ffilter(uncurry(lambda blob, csum: blob != csum)),
        fmap(identify(uncurry(lambda blob, csum: print(f"CORRUPTION checksum mismatch in blob {blob} got {csum}")))),
        fmap(first),
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
            # TODO take remote as a param.
            remotes = vol.remotedb.list()
            remote = None
            if len(remotes) > 0:
                remote = vol.remotedb.read(remotes[0])
            fsck_scanners = {
                '--missing': (fsck_missing_blobs, 1, fsck_fix_missing_blobs),
                '--frozen-ignored': (fsck_frozen_ignored, 4, fsck_fix_frozen_ignored),
                '--blob-permissions': (fsck_blob_permissions, 8, fsck_fix_blob_permissions),
                '--checksums': (fsck_checksum_mismatches, 2, fsck_fix_checksum_mismatches),
            }
            fsck_tasks = [action for (verb, action) in fsck_scanners.items() if args[verb]]
            if len(fsck_tasks) == 0:
                # No options were specified, run the whole suite.
                fsck_tasks = fsck_scanners.values()
            for scanner, fail_code, fixer in fsck_tasks:
                if args['--fix']:
                    foo = pipeline(fixer(vol, remote))(scanner(vol, cwd))
                else:
                    foo = scanner(vol, cwd)
                task_fail_count = count(foo)
                if task_fail_count > 0:
                    exitcode = exitcode | fail_code
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
                force = args['--force']
                if args['delete']:
                    snapdb.delete(name)
                elif args['make']:
                    snapdb.write(name, vol.tree(), force)
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
                force = args['--force']
                remote_vol = getvol(userPath2Path(args['<root>'], cwd))
                vol.remotedb.write(args['<remote>'], remote_vol, force)
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
      farmdbg key write [--force] <key> <value>
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
      farmdbg (s3|api) list <endpoint>
      farmdbg (s3|api) upload (local|userdata|snap <snapshot>) [--quiet] <endpoint>
      farmdbg (s3|api) download userdata [--quiet] <endpoint>
      farmdbg (s3|api)  check <endpoint>
      farmdbg (s3|api) read <endpoint> <blob>...
      farmdbg redact pattern [--noop] <pattern> <from>
    """

def get_remote_bs(args):
    connStr = args['<endpoint>']
    if args['s3']:
        access_id, secret_key = load_s3_creds(None)
        remote_bs = S3Blobstore(connStr, access_id, secret_key)
    elif args['api']:
        remote_bs = HttpBlobstore(connStr, 300)
    else:
        raise ValueError("Must be either s3 or api request")
    return remote_bs

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
            force = args['--force']
            value = args['<value>']
            db.write(key, value, force)
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
            getSrcHandleFn = lambda: remote.bs.read_handle(b)
            vol.bs.import_via_fd(getSrcHandleFn, b)
        else:
            pass  # b exists, can we check its checksum?
        ensure_symlink(f, vol.bs.blob_path(b))
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
            paths = sorted(map(lambda pathStr: vol.root.join(pathStr), pathStrs))
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
            fmap(uncurry(lambda csum, items: (csum, list(map(lambda item: item.pathStr(), items))))),
            fmap(uncurry(missing_printr)),
            count
        )(snapNames)
        if missing_csum2pathStr > 0:
            exitcode = exitcode | 4
    elif args['blobtype']:
        for blob in args['<blob>']:
            blob = ingest(blob)
            print(
                blob,
                maybe("unknown", vol.bs.blob_path(blob).filetype()))
    elif args['blob']:
        if args['path']:
            for csum in args['<blob>']:
                csum = ingest(csum)
                print(csum, vol.bs.blob_path(csum).relative_to(cwd))
        elif args['read']:
            for csum in args['<blob>']:
                with vol.bs.read_handle(csum) as srcFd:
                    copyfileobj(srcFd, getBytesStdOut())
    elif args['s3'] or args['api']:
        quiet = args.get('--quiet')
        remote_bs = get_remote_bs(args)
        def download(blob):
            vol.bs.import_via_fd(lambda: remote_bs.read_handle(blob), blob)
            return blob
        def upload(blob):
            remote_bs.import_via_fd(lambda: vol.bs.read_handle(blob), blob)
            return blob
        if args['list']:
            remote_blobs_iter = remote_bs.blobs()()
            doer = pipeline(fmap(print), consume)
            doer(remote_blobs_iter)
        elif args['upload']:
            print("Calculating remote blobs")
            remote_blobs_iter = remote_bs.blobs()()
            remote_blobs = set(remote_blobs_iter)
            print(f"Remote Blobs: {len(remote_blobs)}")
            if args['local']:
                local_blobs_iter = pipeline(
                        ffilter(lambda x: x.is_link()),
                        fmap(lambda x: x.csum()),
                        uniq)(iter(vol.tree()))
            elif args['userdata']:
                local_blobs_iter = vol.bs.blobs()
            elif args['snap']:
                snap_name = args['<snapshot>']
                local_blobs_iter = pipeline(
                    ffilter(lambda x: x.is_link()),
                    fmap(lambda x: x.csum()),
                    uniq)(iter(vol.snapdb.read(snap_name)))
            print("Calculating local blobs")
            local_blobs = set(local_blobs_iter)
            print(f"Local Blobs: {len(local_blobs)}")
            transfer_blobs = local_blobs - remote_blobs
            with tqdm(desc="Uploading to remote", disable=quiet, total=len(transfer_blobs), smoothing=1.0, dynamic_ncols=True, maxinterval=1.0) as pbar:
                def update_pbar(blob):
                    pbar.update(1)
                    pbar.set_description("Uploaded %s" % blob)
                print(f"Uploading {len(transfer_blobs)} blobs to remote")
                all_success = pipeline(
                    pfmaplazy(upload, workers=2),
                    fmap(identify(update_pbar)),
                    partial(every, identity),
                )(transfer_blobs)
                if all_success:
                    print("Successfully uploaded")
                else:
                    print("Failed to upload")
                    exitcode = exitcode | 1
        elif args['download']:
            if args['userdata']:
                remote_blobs_iter = remote_bs.blobs()()
                local_blobs_iter = vol.bs.blobs()
            else:
                raise ValueError("Invalid download source")
            print("Calculating remote blobs")
            remote_blobs = set(remote_blobs_iter)
            print(f"Remote Blobs: {len(remote_blobs)}")
            print(f"Calculating local blobs")
            local_blobs = set(local_blobs_iter)
            print(f"Local Blobs: {len(local_blobs)}")
            transfer_blobs = remote_blobs - local_blobs
            with tqdm(desc="downloading from remote", disable=quiet, total=len(transfer_blobs), smoothing=1.0, dynamic_ncols=True, maxinterval=1.0) as pbar:
                def update_pbar(blob):
                    pbar.update(1)
                    pbar.set_description(f"Downloaded {blob}")
                print(f"downloading {len(transfer_blobs)} blobs from remote")
                all_success = pipeline(
                    pfmaplazy(download, workers=2),
                    fmap(identify(update_pbar)),
                    partial(every, identity),
                )(transfer_blobs)
                if all_success:
                    print("Successfully downloaded")
                else:
                    print("Failed to download")
                    exitcode = exitcode | 1
        elif args['check']:  # TODO what are the check semantics for API? Weird to look at etag.
            if args['s3']:
                num_corrupt_blobs = pipeline(
                    ffilter(lambda obj: obj['ETag'][1:-1] != obj['blob']),
                    fmap(identify(lambda obj: print(obj['blob'], obj['ETag'][1:-1]))),
                    count
                )(remote_bs.blob_stats()())  # TODO blob_stats is s3 only.
            elif args['api']:
                num_corrupt_blobs = pipeline(
                    fmap(lambda blob: [blob, remote_bs.blob_checksum(blob)]),
                    ffilter(lambda blob_csum: blob_csum[0] != blob_csum[1]),
                    fmap(identify(lambda blob_csum: print(blob_csum[0], blob_csum[1]))),
                    count
                )(remote_bs.blobs()())
            if num_corrupt_blobs == 0:
                print("All remote blobs etags match")
            else:
                exitcode = exitcode | 2
        elif args['read']:
            for blob in args.get('<blob>'):
                with remote_bs.read_handle(blob) as srcFd:
                    copyfileobj(srcFd, getBytesStdOut())
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
            vol.snapdb.write(snapName, out_snap, True)
    return exitcode
