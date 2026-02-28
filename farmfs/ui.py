from __future__ import print_function
from posixpath import sep
from typing import (Any, BinaryIO, Callable, Dict, Generator, IO, Iterable, Iterator,
                    List, Never, Optional, Set, Tuple, cast)
from farmfs import getvol
from docopt import docopt
from farmfs import cwd
from farmfs.snapshot import KeySnapshot, SnapDelta, Snapshot, SnapshotItem
from farmfs.util import (
    cardinality,
    concat,
    concatMap,
    consume,
    count,
    copyfileobj,
    csum_pct,
    empty_default,
    every_pred,
    ffilter,
    fgroupby,
    finvert,
    fmap,
    groupby,
    identify,
    ingest,
    maybe,
    partial,
    pfmaplazy,
    pipeline,
    uncurry,
    uniq,
    zipFrom,
)
from farmfs.keydb import KeyDBLike
from farmfs.volume import (BlobOperation, FarmFSVolume, ImportResult, TreeDescription,
                           TreeOperation, VolumeChangeOperation, mkfs, tree_diff,
                           tree_patcher, encode_snapshot)
from farmfs.fs import (
    Path,
    WalkItem,
    userPath2Path,
    ftype_selector,
    LINK,
    ignored_path_checker,
    walk,
    ensure_symlink,
    walk_path,
)
from json import JSONEncoder
from s3lib.ui import load_creds as load_s3_creds
import sys
import tqdm as tqdmlib
from farmfs.blobstore import FileBlobstore, S3Blobstore, HttpBlobstore
from farmfs.progress import csum_pbar, lazy_pbar, list_pbar, tree_pbar

def noop(x: Any) -> None:
    return None

def getBytesStdOut() -> BinaryIO:
    "On python 3+, sys.stdout.buffer is bytes writable."
    return sys.stdout.buffer


json_encoder = JSONEncoder(ensure_ascii=False, sort_keys=True)
json_encode = lambda data: json_encoder.encode(data)
json_printr = pipeline(json_encode, print)
def jsons_printr(xs: Iterable[Any]) -> None:
    "Print an iterable of any data as a json list."
    json_printr(xs)


strs_printr = pipeline(fmap(print), consume)


def dict_printr(keys: Iterable[str], d: dict[str, str | bytes]) -> None:
    vals = (d.get(k, "") for k in keys)
    strs = (ingest(v) for v in vals)
    print("\t".join(strs))


def dicts_printr(keys: Iterable[str]) -> Callable[[Iterable[Dict[str, str | bytes]]], None]:
    printr = partial(dict_printr, keys)
    seq_printr = fmap(printr)
    pipe = pipeline(seq_printr, consume)
    return pipe

# TODO its like a need a volume.trees() which returns the tree, then the snaps!
# TODO dead code
def snap_reader(vol: FarmFSVolume) -> Callable[[str], Snapshot]:
    def snap_reader_impl(snap_name: str) -> Snapshot:
        if snap_name == "<tree>":
            return vol.tree()
        else:
            return vol.snapdb.read(snap_name)
    return snap_reader_impl

# TODO this is just zipFrom.
def snap_flattener(tree: Snapshot) -> Iterator[Tuple[Snapshot, SnapshotItem]]:
    return zipFrom(tree, iter(tree))


snapshot_printr = dicts_printr(["path", "type", "csum"])


UI_USAGE = """
FarmFS

Usage:
  farmfs mkfs [options] [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [options] [<path>...]
  farmfs snap list [options]
  farmfs snap (make|read|delete|restore|diff) [options] [--force] <snap>
  farmfs fsck [options] [--missing --frozen-ignored --blob-permissions --checksums --keydb] [--fix]
  farmfs count [options]
  farmfs similarity [options] <dir_a> <dir_b>
  farmfs gc [options] [--noop]
  farmfs remote add [options] [--force] <remote> <root>
  farmfs remote remove [options] <remote>
  farmfs remote list [options] [<remote>]
  farmfs pull [options] <remote> [<snap>]
  farmfs diff [options] <remote> [<snap>]
  farmfs fetch [options] [--force] [<remote>] [<snap>]


Options:
  --quiet  Disable progress bars.

"""


def shorten_str(s: str, max: int, suffix: str = "..."):
    if len(s) > max - len(suffix):
        return s[0: max - len(suffix)] + suffix
    return s


def snap_item_progress(label: str, quiet: bool, leave: bool):

    @uncurry
    def snap_item_desc(snap: Snapshot, item: SnapshotItem) -> str:
        snap_name = snap.name
        path_str = item.pathStr()
        return shorten_str(f"{snap_name} : {path_str}", 35)

    return tree_pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=snap_item_desc,
    )


def link_item_progress(label: str, quiet: bool, leave: bool, cwd: Path):

    def link_item_desc(walk_item: WalkItem) -> str:
        path, ftype = walk_item
        path_str = path.relative_to(cwd)
        return shorten_str(str(path_str), 35)

    """Progress bar for link/path items."""
    return tree_pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=link_item_desc,
    )


def blob_stats_progress(label: str, quiet: bool) -> Callable[[Iterable], Generator]:
    """Progress bar for blob_stats objects, using 'blob' field for cardinality estimation.

    Handles objects returned by blobstore.blob_stats() which have a 'blob' key containing
    the checksum. The checksum is used for progress estimation while the full object flows
    through the pipeline.
    """
    from farmfs.progress import pbar

    def _postfix(obj: dict) -> str:
        return obj["blob"]

    def _cardinality(idx: int, obj: dict) -> int:
        csum = obj["blob"]
        pct = csum_pct(csum)
        return cardinality(idx, pct)

    return pbar(
        label=label,
        quiet=quiet,
        leave=True,
        postfix=_postfix,
        force_refresh=False,
        total=float("inf"),
        cardinality_fn=_cardinality,
    )


def op_doer(op: Tuple[BlobOperation, TreeOperation, TreeDescription]) -> None:
    (blob_op, tree_op, desc) = op
    blob_op()
    tree_op()


stream_op_doer = fmap(op_doer)


def fsck_fix_missing_blobs(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
) -> Callable[[Iterable[Tuple[str, Iterable[Tuple[Snapshot, SnapshotItem]]]]], Iterable[str]]:
    if remote is None:
        raise ValueError("No remote specified, cannot restore missing blobs")

    @uncurry
    def select_csum(csum: str, snap_items: Iterable[Tuple[Snapshot, SnapshotItem]]) -> str:
        return csum
    select_csums = fmap(select_csum)

    def download_missing_blob(csum: str) -> str:
        getSrcHandleFn = lambda: remote.bs.read_handle(csum)
        vol.bs.import_via_fd(getSrcHandleFn, csum)
        return csum
    download_missing_blobs = fmap(download_missing_blob)

    def printr(csum: str) -> str:
        print("\tRestored ", csum, "from remote")
        return csum
    printrs = fmap(printr)
    pipe = pipeline(select_csums, download_missing_blobs, printrs)
    return pipe


def fsck_tree_source(vol: FarmFSVolume, cwd: Path) -> Iterator[Tuple[Snapshot, SnapshotItem]]:
    trees = vol.trees()
    def tree_items(t: Snapshot) -> Iterator[Tuple[Snapshot, SnapshotItem]]:
        return zipFrom(t, iter(t))
    trees_items = concatMap(tree_items)
    return pipeline(trees_items)(trees)


# TODO what is return type?
def fsck_missing_blobs(vol: FarmFSVolume, cwd: Path):
    """Look for blobs in tree or snaps which are not in blobstore."""
    def is_link(snap: Snapshot, item: SnapshotItem) -> bool:
        return item.is_link()
    is_link_tuple = uncurry(is_link)
    tree_links = ffilter(is_link_tuple)
    def is_missing(snap: Snapshot, item: SnapshotItem) -> bool:
        return not vol.bs.exists(item.csum())
    is_missing_tuple = uncurry(is_missing)
    broken_tree_links = ffilter(is_missing_tuple)
    def get_csum(snap: Snapshot, item: SnapshotItem) -> str:
        return item.csum()
    get_csum_tuple = uncurry(get_csum)
    checksum_grouper = fgroupby(get_csum_tuple)

    def broken_link_printr(csum: str, snap_items: Iterable[Tuple[Snapshot, SnapshotItem]]) -> None:
        print(csum)
        for snap, item in snap_items:
            print("", snap.name, item.to_path(vol.root).relative_to(cwd), sep="\t")
    broken_link_printr_tuple = uncurry(broken_link_printr)
    identity_broken_link_printr_tuple = identify(broken_link_printr_tuple)
    broken_links_printr = fmap(identity_broken_link_printr_tuple)
    bad_blobs_checker = pipeline(
        tree_links, broken_tree_links, checksum_grouper, broken_links_printr
    )
    return bad_blobs_checker

# TODO weird signature, iterable None
def fsck_fix_frozen_ignored(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
) -> Callable[[Iterable[Path]], Iterable[None]]:
    """Thaw out files in the tree which are ignored."""
    fixer = fmap(vol.thaw)

    def printr(p: Path) -> None:
        print("Thawed", p.relative_to(vol.root))

    printrs = fmap(printr)
    pipe = pipeline(fixer, printrs)
    return pipe


def fsck_vol_root_source(vol: FarmFSVolume, cwd: Path) -> Generator[WalkItem, None, None]:
    ignore_mdd = ignored_path_checker([str(vol.mdd)])
    return walk(vol.root, skip=ignore_mdd)


def fsck_frozen_ignored(
        vol: FarmFSVolume,
        cwd: Path
) -> Callable[[Iterable[WalkItem]], Iterable[Path]]:
    """Look for frozen links which are in the ignored file."""
    # TODO some of this logic could be moved to volume.
    #      Which files are members of the volume is a function of the volume.
    keep_links = ftype_selector([LINK])
    just_path = fmap(walk_path)
    keep_ignored = ffilter(vol.is_ignored)

    def print_path(p: Path) -> Path:
        print("Ignored file frozen:", p.relative_to(cwd))
        return p
    print_paths = fmap(print_path)
    ignored_frozen_checker = pipeline(
        keep_links,
        just_path,
        keep_ignored,
        print_paths,
    )
    return ignored_frozen_checker


# TODO weird signature, iterable None
def fsck_fix_blob_permissions(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume]
) -> Callable[[Iterable[str]], Iterable[None]]:
    fixer = fmap(identify(vol.bs.fix_blob_permissions))
    printr = fmap(lambda blob: print("fixed blob permissions:", blob))
    return pipeline(fixer, printr)


def fsck_blob_permissions(vol: FarmFSVolume, cwd: Path
                          ) -> Callable[[Iterable[str]], Iterable[str]]:
    """Look for blobstore blobs which are not readonly."""
    blob_permissions_checker = pipeline(
        ffilter(finvert(vol.bs.verify_blob_permissions)),
        fmap(identify(partial(print, "writable blob: "))),
    )
    return blob_permissions_checker


# TODO if the corruption fix fails, we don't fail the command.
def fsck_fix_checksum_mismatches(vol: FarmFSVolume, remote: Optional[FarmFSVolume]
                                 ) -> Callable[[Iterable[str]], Iterable[str]]:
    if remote is None:
        raise ValueError("No remote specified, cannot restore missing blobs")

    def checksum_fixer(blob: str) -> None:
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


def fsck_blob_source(vol: FarmFSVolume, cwd: Path) -> Iterator[str]:
    return vol.bs.blobs()


def fsck_checksum_mismatches(vol: FarmFSVolume, cwd: Path) -> Callable[[Iterable[str]], Iterable[str]]:
    """Look for checksum mismatches."""
    # TODO CORRUPTION checksum mismatch in blob <CSUM>, would be nice to know back references.
    def blob_calc_checksum(blob: str) -> Tuple[str, str]:
        return blob, vol.bs.blob_checksum(blob)
    p_blob_calc_checksums = pfmaplazy(blob_calc_checksum)
    def blob_is_corrupt(blob: str, checksum: str) -> bool:
        """Return True if corrupt, False if correct."""
        return blob != checksum
    blob_is_curript_tuple = uncurry(blob_is_corrupt)
    def corrupt_printer(blob: str, csum: str) -> str:
        print(f"CORRUPTION checksum mismatch in blob {blob} got {csum}")
        return blob
    corrupt_printer_tuple = uncurry(corrupt_printer)
    corrupt_printer_tuples = fmap(corrupt_printer_tuple)

    checker = pipeline(
        p_blob_calc_checksums,
        ffilter(blob_is_curript_tuple),
        corrupt_printer_tuples,
    )
    return checker


FsckCheck = Callable[[], Tuple[Iterable[Any], int]]


def fsck_check_missing(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
        quiet: bool,
        fix: bool,
        cwd: Path
) -> Tuple[Iterable[Any], int]:
    snap_count = len(vol.snapdb.list()) + 1  # +1 for the live tree; cheap key listing, no data read

    def snap_name(s: Snapshot) -> str:
        return s.name

    snaps = list_pbar(
        label="Snapshot",
        quiet=quiet,
        leave=False,
        postfix=snap_name,
        force_refresh=True,
        total=snap_count)(vol.trees())

    @uncurry
    def snap_item_desc(snap: Snapshot, item: SnapshotItem) -> str:
        return shorten_str(f"{snap.name} : {item.pathStr()}", 35)

    missing: Iterable[Tuple[str, Iterable[Tuple[Snapshot, SnapshotItem]]]] = pipeline(
        concatMap(snap_flattener),
        lazy_pbar(tree_pbar(label="checking blobs", quiet=quiet, leave=False, postfix=snap_item_desc)),
        fsck_missing_blobs(vol, cwd),
    )(snaps)
    if fix:
        return fsck_fix_missing_blobs(vol, remote)(missing), 1
    return missing, 1


def fsck_check_frozen_ignored(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
        quiet: bool,
        fix: bool,
        cwd: Path) -> Tuple[Iterable[Any], int]:
    def link_item_desc(walk_item: WalkItem) -> str:
        path, ftype = walk_item
        return shorten_str(str(path.relative_to(cwd)), 35)

    frozen_ignored: Iterable[Path] = pipeline(
        tree_pbar(label="Frozen Ignored", quiet=quiet, leave=False, postfix=link_item_desc),
        fsck_frozen_ignored(vol, cwd),
    )(fsck_vol_root_source(vol, cwd))
    if fix:
        return fsck_fix_frozen_ignored(vol, remote)(frozen_ignored), 4
    return frozen_ignored, 4


def fsck_check_blob_permissions(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
        quiet: bool,
        fix: bool,
        cwd: Path) -> Tuple[Iterable[Any], int]:
    bad_perms: Iterable[str] = pipeline(
        csum_pbar(label="Blob Permissions", quiet=quiet, leave=False),
        fsck_blob_permissions(vol, cwd),
    )(fsck_blob_source(vol, cwd))
    if fix:
        return fsck_fix_blob_permissions(vol, remote)(bad_perms), 8
    return bad_perms, 8


def fsck_check_checksums(
        vol: FarmFSVolume,
        remote: Optional[FarmFSVolume],
        quiet: bool,
        fix: bool,
        cwd: Path) -> Tuple[Iterable[Any], int]:
    corrupt: Iterable[str] = pipeline(
        csum_pbar(label="Checksums", quiet=quiet, leave=False),
        fsck_checksum_mismatches(vol, cwd),
    )(fsck_blob_source(vol, cwd))
    if fix:
        return fsck_fix_checksum_mismatches(vol, remote)(corrupt), 2
    return corrupt, 2


def fsck_check_keydb(vol: FarmFSVolume,
                     remote: Optional[FarmFSVolume],
                     quiet: bool,
                     fix: bool,
                     cwd: Path) -> Tuple[Iterable[Any], int]:
    errors: List[str] = []

    def run_storage() -> None:
        blob_keys = vol.blob_db.list()
        for key in list_pbar(label="  Storage", quiet=quiet, leave=False, postfix=lambda k: str(k), total=len(blob_keys))(blob_keys):
            try:
                ok = vol.blob_db.verify(key)
            except FileNotFoundError:
                print(f"CORRUPT keydb key: {key} (dangling symlink)")
                errors.append(key)
                continue
            if not ok:
                print(f"CORRUPT keydb key: {key} (checksum mismatch)")
                errors.append(key)

    def run_json() -> None:
        json_keys = vol.keydb.list()
        for key in list_pbar(label="  JSON", quiet=quiet, leave=False, postfix=lambda k: str(k), total=len(json_keys))(json_keys):
            if key in errors:
                continue
            try:
                detail = vol.keydb.diagnose(key)
            except FileNotFoundError:
                pass  # already caught at bytes level
            else:
                if detail:
                    if fix:
                        vol.keydb.rewrite(key)
                        print(f"FIXED keydb key: {key} (rewritten in canonical JSON)")
                    else:
                        print(f"CORRUPT keydb key: {key} (JSON round-trip failed)")
                        for line in detail:
                            print(f"  {line}")
                        errors.append(key)

    def run_semantic() -> None:
        factories: List[Tuple[str, KeyDBLike]] = [("snaps", vol.snapdb), ("remotes", vol.remotedb)]
        for name, factory in factories:
            factory_keys = factory.list()
            for key in list_pbar(label=f"  Semantic/{name}", quiet=quiet, leave=False, postfix=lambda k: str(k), total=len(factory_keys))(factory_keys):
                full_key = name + sep + key
                if full_key in errors:
                    continue
                try:
                    detail = factory.diagnose(key)
                except FileNotFoundError:
                    pass
                else:
                    if detail:
                        if fix:
                            vol.keydb.rewrite(name + sep + key)
                            print(f"FIXED keydb key: {full_key} (rewritten in canonical JSON)")
                        else:
                            print(f"CORRUPT keydb key: {full_key} (semantic validation failed)")
                            for line in detail:
                                print(f"  {line}")
                            errors.append(full_key)

    stages: List[Tuple[str, Callable[[], None]]] = [
        ("Storage", run_storage),
        ("JSON", run_json),
        ("Semantic", run_semantic),
    ]
    def stage_name(s: Tuple[str, Callable[[], None]]) -> str:
        return s[0]
    for _stage, run in list_pbar(label="keydb", quiet=quiet, leave=False, postfix=stage_name, total=len(stages))(stages):
        run()

    return iter(errors), 16


def ui_main() -> Never:
    result = farmfs_ui(sys.argv[1:], cwd)
    exit(result)


def farmfs_ui(argv: List[str], cwd: Path) -> int:

    def rel_path(p: Path) -> str:
        "Convert absolute path to path relative to cwd for display purposes."
        return p.relative_to(cwd)

    exitcode = 0
    args = docopt(UI_USAGE, argv)
    quiet = args.get("--quiet")
    if args["mkfs"]:
        root = userPath2Path(args["<root>"] or ".", cwd)
        udd_path = (
            userPath2Path(args["<data>"], cwd)
            if args.get("<data>")
            else Path(".farmfs/userdata", root)
        )
        mkfs(root, udd_path)
        print("FileSystem Created %s using blobstore %s" % (root, udd_path))
    else:
        vol = getvol(cwd)
        paths = empty_default(
            map(lambda x: userPath2Path(x, cwd), args["<path>"]), [vol.root]
        )

        def delta_printr(delta: SnapDelta) -> SnapDelta:
            deltaPath = delta.path(vol.root).relative_to(cwd)
            print("diff: %s %s %s" % (delta.mode, deltaPath, delta.csum))
            return delta

        stream_delta_printr = fmap(delta_printr)

        def op_printr(op: VolumeChangeOperation) -> None:
            (blob_op, tree_op, (desc, path)) = op
            print(desc % rel_path(path))

        stream_op_printr = fmap(identify(op_printr))
        if args["status"]:
            get_thawed = fmap(vol.thawed)
            pipeline(
                get_thawed,
                concat,
                fmap(rel_path),
                fmap(print),
                consume,
            )(paths)
        elif args["freeze"]:

            def printr(freeze_op: ImportResult) -> None:
                s = "Imported %s with checksum %s" % (
                    freeze_op["path"].relative_to(cwd),
                    freeze_op["csum"],
                )
                if freeze_op["was_dup"]:
                    print(s, "was a duplicate")
                else:
                    print(s)

            importer = fmap(vol.freeze)
            get_thawed = fmap(vol.thawed)
            print_list = fmap(printr)
            pipeline(get_thawed, concat, importer, print_list, consume)(paths)
        elif args["thaw"]:

            def thaw_printr(path: Path) -> None:
                print("Exported %s" % rel_path(path))

            exporter = fmap(vol.thaw)
            get_frozen = fmap(vol.frozen)
            thaw_print_list = fmap(thaw_printr)
            pipeline(get_frozen, concat, exporter, thaw_print_list, consume)(paths)
        elif args["fsck"]:
            # TODO take remote as a param.
            remotes = vol.remotedb.list()
            remote = None
            if len(remotes) > 0:
                remote = vol.remotedb.read(remotes[0])
            fix = bool(args["--fix"])
            fsck_checks: List[Tuple[str, FsckCheck]] = [
                ("missing", lambda: fsck_check_missing(vol, remote, quiet, fix, cwd)),
                ("frozen-ignored", lambda: fsck_check_frozen_ignored(vol, remote, quiet, fix, cwd)),
                ("blob-permissions", lambda: fsck_check_blob_permissions(vol, remote, quiet, fix, cwd)),
                ("checksums", lambda: fsck_check_checksums(vol, remote, quiet, fix, cwd)),
                ("keydb", lambda: fsck_check_keydb(vol, remote, quiet, fix, cwd)),
            ]
            selected: List[Tuple[str, FsckCheck]] = [
                (name, check)
                for (name, check) in fsck_checks
                if args.get(f"--{name}")
            ]
            if len(selected) == 0:
                selected = fsck_checks
            @uncurry
            def fsck_task_name(name: str, check: FsckCheck) -> str:
                return name
            tasks_bar = list_pbar(label="Running fsck tasks", quiet=quiet, postfix=fsck_task_name, force_refresh=True)
            for name, check in tasks_bar(selected):
                fails, code = check()
                if count(fails) > 0:
                    exitcode = exitcode | code
        elif args["count"]:
            trees = vol.trees()
            tree_items = concatMap(snap_flattener)
            tree_links = ffilter(uncurry(lambda snap, item: item.is_link()))
            checksum_grouper = partial(groupby, uncurry(lambda snap, item: item.csum()))

            def count_printr(csum, snap_items):
                print(csum, count(snap_items))
                for snap, item in snap_items:
                    print(snap.name, item.to_path(vol.root).relative_to(cwd))

            counts_printr = fmap(identify(uncurry(count_printr)))
            pipeline(tree_items, tree_links, checksum_grouper, counts_printr, consume)(
                trees
            )
        elif args["similarity"]:
            dir_a = userPath2Path(args["<dir_a>"], cwd)
            dir_b = userPath2Path(args["<dir_b>"], cwd)
            print("left", "both", "right", "jaccard_similarity", sep="\t")
            print(*vol.similarity(dir_a, dir_b), sep="\t")
        elif args["gc"]:
            applyfn: Callable[[Iterable[str]], Iterable[None]]
            if args.get("--noop"):
                applyfn = fmap(noop)
            else:
                applyfn = fmap(vol.bs.delete_blob)
            @fmap
            def remove_printr(blob: str) -> str:
                print("Removing", blob)
                return blob
            # Actually print and do the delete (if not noop).
            remove_pipe = pipeline(
                remove_printr,
                applyfn,
                consume
            )
            pipeline(remove_pipe)(sorted(vol.unused_blobs(vol.items())))
        elif args["snap"]:
            snapdb = vol.snapdb
            if args["list"]:
                # TODO have an optional argument for which remote.
                print("\n".join(snapdb.list()))
            else:
                name = args["<snap>"]
                force = args["--force"]
                if args["delete"]:
                    snapdb.delete(name)
                elif args["make"]:
                    snapdb.write(name, cast(KeySnapshot, vol.tree()), force)
                else:
                    snap = snapdb.read(name)
                    if args["read"]:
                        for i in snap:
                            print(i)
                    elif args["restore"]:
                        diff = tree_diff(vol.tree(), snap)
                        pipeline(
                            stream_delta_printr,
                            tree_patcher(vol, vol),
                            stream_op_printr,
                            stream_op_doer,
                            consume,
                        )(diff)
                    elif args["diff"]:
                        diff = tree_diff(vol.tree(), snap)
                        pipeline(stream_delta_printr, consume)(diff)
        elif args["remote"]:
            if args["add"]:
                force = args["--force"]
                remote_vol = getvol(userPath2Path(args["<root>"], cwd))
                vol.remotedb.write(args["<remote>"], remote_vol, force)
            elif args["remove"]:
                vol.remotedb.delete(args["<remote>"])
            elif args["list"]:
                if args["<remote>"]:
                    remote_vol = vol.remotedb.read(args["<remote>"])
                    print("\n".join(remote_vol.snapdb.list()))
                else:
                    for remote_name in vol.remotedb.list():
                        remote_vol = vol.remotedb.read(remote_name)
                        print(remote_name, remote_vol.root)
        elif args["pull"] or args["diff"]:
            remote_vol = vol.remotedb.read(args["<remote>"])
            snap_name = args["<snap>"]
            remote_snap = (
                remote_vol.snapdb.read(snap_name) if snap_name else remote_vol.tree()
            )
            diff = tree_diff(vol.tree(), remote_snap)
            if args["pull"]:
                patcher = tree_patcher(vol, remote_vol)
                pipeline(
                    stream_delta_printr,
                    patcher,
                    stream_op_printr,
                    stream_op_doer,
                    consume,
                )(diff)
            else:  # diff
                pipeline(stream_delta_printr, consume)(diff)
        elif args["fetch"]:
            remote_name = args["<remote>"]
            snap_name = args["<snap>"]
            force = bool(args["--force"])
            remote_names: List[str] = [str(remote_name)] if remote_name else vol.remotedb.list()

            def blob_postfix(item: SnapshotItem) -> str:
                return shorten_str(str(item.to_path(vol.root).relative_to(cwd)), 35)

            def fetch_one(rname: str, sname: str) -> int:
                remote_vol = vol.remotedb.read(rname)
                local_name = rname + sep + sname
                try:
                    remote_raw = remote_vol.blob_db.read("snaps" + sep + sname)
                except FileNotFoundError:
                    raise ValueError("Snap %r not found on remote %r" % (sname, rname))
                try:
                    local_raw: Optional[bytes] = vol.blob_db.read("snaps" + sep + local_name)
                except FileNotFoundError:
                    local_raw = None
                if local_raw is not None:
                    if remote_raw == local_raw:
                        tqdmlib.tqdm.write("Already up to date: %s" % local_name)
                        return 0
                    elif not force:
                        tqdmlib.tqdm.write("Error: %s has diverged; use --force to overwrite" % local_name)
                        return 32
                    else:
                        tqdmlib.tqdm.write("Overwriting %s/%s" % (rname, sname))
                remote_snap = remote_vol.snapdb.read(sname)
                remote_items = list(remote_snap)
                pbar = tree_pbar(label=sname, quiet=quiet, leave=False, postfix=blob_postfix)
                for item in pbar(remote_items):
                    if item.is_link():
                        csum = item.csum()
                        if not vol.bs.exists(csum):
                            vol.bs.import_via_fd(lambda: remote_vol.bs.read_handle(csum), csum)
                vol.snapdb.write(local_name, KeySnapshot(remote_items, local_name, vol.bs.reverser), force)
                tqdmlib.tqdm.write("Fetched %s/%s as %s" % (rname, sname, local_name))
                return 0

            def fetch_remote(rname: str) -> int:
                remote_vol = vol.remotedb.read(rname)
                snap_names: List[str] = [str(snap_name)] if snap_name else remote_vol.snapdb.list()
                snap_pbar = list_pbar(label=rname, quiet=quiet, postfix=str, total=len(snap_names))
                rc = 0
                for sname in snap_pbar(snap_names):
                    rc = rc | fetch_one(rname, str(sname))
                return rc

            for rname in remote_names:
                exitcode = exitcode | fetch_remote(rname)
    return exitcode


def printNotNone(value: Optional[object]) -> None:
    if value is not None:
        print(value)


DBG_USAGE = """
    FarmDBG

    Usage:
      farmdbg fs reverse [options] [--snap=<snapshot>|--all] <csum>...
      farmdbg key read [options] <key>
      farmdbg key write [options] [--force] <key> <value>
      farmdbg key delete [options] <key>
      farmdbg key list [options] [<query>]
      farmdbg key path [options] <key>
      farmdbg walk (keys|userdata|root|snap <snapshot>) [options] [--json]
      farmdbg checksum [options] <path>...
      farmdbg fix link [options] [--remote=<remote>] <target> <file>
      farmdbg rewrite-links [options]
      farmdbg missing [options] <snap>...
      farmdbg blob path [options] <blob>...
      farmdbg blob read [options] [--output=<outfile>] <blob>...
      farmdbg blob type [options] <blob>...
      farmdbg blob reverse [options] <path>...
      farmdbg (s3|api|file) list [options] <endpoint>
      farmdbg (s3|api|file) upload (local|userdata|snap <snapshot>) [options] <endpoint>
      farmdbg (s3|api|file) download userdata [options] <endpoint>
      farmdbg (s3|api|file) check [options] <endpoint>
      farmdbg (s3|api|file) read [options] [--output=<outfile>] <endpoint> <blob>...
      farmdbg redact pattern [options] [--noop] <pattern> <from>

    Options:
      --quiet  Disable progress bars.
    """


def get_remote_bs(args: dict[str, str], cwd: Path) -> FileBlobstore | HttpBlobstore | S3Blobstore:
    connStr = args["<endpoint>"]
    if args["s3"]:
        access_id, secret_key = load_s3_creds(None)
        return S3Blobstore(connStr, access_id, secret_key)
    elif args["api"]:
        return HttpBlobstore(connStr, 300)
    elif args["file"]:
        return getvol(userPath2Path(connStr, cwd)).bs
    else:
        raise ValueError("Must be s3, api, or file")


def dbg_main():
    return dbg_ui(sys.argv[1:], cwd)


def dbg_ui(argv: list[str], cwd: Path) -> int:
    exitcode = 0
    args = docopt(DBG_USAGE, argv)
    quiet = args.get("--quiet")
    vol = getvol(cwd)
    if args["fs"]:
        if args["reverse"]:
            csums = args["<csum>"]
            if args["--all"]:
                trees = vol.trees()
            elif args["--snap"]:
                snap_tree: Snapshot = vol.snapdb.read(args["--snap"])
                trees = iter([snap_tree])
            else:
                trees = iter([vol.tree()])
            tree_items = concatMap(snap_flattener)

            @uncurry
            def item_is_link(snap: Snapshot, item: SnapshotItem) -> bool:
                return item.is_link()
            tree_links = ffilter(item_is_link)

            @uncurry
            def csum_in_set(snap: Snapshot, item: SnapshotItem) -> bool:
                return item.csum() in csums
            matching_links = ffilter(csum_in_set)

            @uncurry
            def link_printr(snap: Snapshot, item: SnapshotItem) -> None:
                print(item.csum(), snap.name, item.to_path(vol.root).relative_to(cwd))

            links_printr = fmap(identify(link_printr))
            pipeline(tree_items, tree_links, matching_links, links_printr, consume)(trees)
    elif args["key"]:
        blob_db = vol.blob_db
        key = str(args["<key>"])
        if args["read"]:
            try:
                key_val = blob_db.read(key)
                getBytesStdOut().write(key_val)
            except FileNotFoundError:
                pass
        elif args["delete"]:
            blob_db.delete(key)
        elif args["list"]:
            query: str | None = args['<query>']
            for v in blob_db.list(query):
                print(v)
        elif args["write"]:
            force = bool(args["--force"])
            value = args["<value>"]
            vol.keydb.write(key, value, force)
        elif args["path"]:
            print(blob_db.keypath(key).relative_to(cwd))
    elif args["walk"]:
        if args["root"]:
            printr = jsons_printr if args.get("--json") else snapshot_printr
            printr(encode_snapshot(vol.tree()))
        elif args["snap"]:
            # TODO could add a test for output encoding.
            # TODO could add a test for snap format. Leading '/' on paths.
            printr = jsons_printr if args.get("--json") else snapshot_printr
            printr(encode_snapshot(vol.snapdb.read(args["<snapshot>"])))
        elif args["userdata"]:
            blobs = vol.bs.blobs()
            printr = jsons_printr if args.get("--json") else strs_printr
            printr(blobs)
        elif args["keys"]:
            printr = json_printr if args.get("--json") else strs_printr
            printr(vol.keydb.list())
    elif args["checksum"]:
        paths = empty_default(map(lambda x: Path(x, cwd), args["<path>"]), [vol.root])
        for p in paths:
            print(p.checksum(), p.relative_to(cwd))
    elif args["link"]:
        f = Path(args["<file>"], cwd)
        b = ingest(args["<target>"])
        if not vol.bs.exists(b):
            print("blob %s doesn't exist" % b)
            if args["--remote"]:
                remote = vol.remotedb.read(args["--remote"])
            else:
                raise ValueError("aborting due to missing blob")
            getSrcHandleFn = lambda: remote.bs.read_handle(b)
            vol.bs.import_via_fd(getSrcHandleFn, b)
        else:
            pass  # b exists, can we check its checksum?
        ensure_symlink(f, vol.bs.blob_path(b))
    elif args["rewrite-links"]:
        for item in vol.tree():
            if not item.is_link():
                continue
            path = item.to_path(vol.root)
            new = vol.repair_link(path)
            if new is not None:
                print("Relinked %s to %s" % (path.relative_to(cwd), new))
    elif args["missing"]:
        # Are there any entities which are in snaps, but not in the tree?
        # This can happen if we delete data from the tree and could be lost data!
        # Skip ignored files since they are supposed to be deleted without notice.
        def is_link(item: SnapshotItem):
            return item.is_link()
        keep_snap_links = ffilter(is_link)
        def item_csum(item: SnapshotItem) -> str:
            return item.csum()
        item_csums = fmap(item_csum)
        get_root_csums: Callable[[Iterator[SnapshotItem]], Set[str]] = pipeline(
            keep_snap_links,
            item_csums,
            set)
        tree_csums = get_root_csums(iter(vol.tree()))

        # Construct a predicate which determintes if a ShapshotItem is missing,
        # meaking it a link whose csum is not in the tree and is not ignored.
        def is_not_ignored(item: SnapshotItem) -> bool:
            return not vol.is_ignored(item.to_path(vol.root))
        def is_csum_missing(snap_item: SnapshotItem) -> bool:
            """
            We want to return all the items which are in the old snaps which are MISSING from the tree.
            """
            snap_csum = snap_item.csum()
            return snap_csum not in tree_csums
        is_missing_item: Callable[[SnapshotItem], bool] = every_pred(is_link, is_not_ignored, is_csum_missing)
        @uncurry
        def is_missing2(snap: Snapshot, item: SnapshotItem) -> bool:
            return is_missing_item(item)
        @uncurry
        def to_missing_row(snap: Snapshot, item: SnapshotItem) -> Tuple[str, str, str]:
            return item.csum(), snap.name, item.to_path(vol.root).relative_to(cwd)

        missing_item_table = pipeline(
            ffilter(is_missing2),
            fmap(to_missing_row),
            sorted,
        )
        # Lets read all the snaps and collect the missing items.
        snapNames = cast(List[str], args["<snap>"])
        def get_snap_items_from_snap(snapName: str) -> Iterable[Tuple[Snapshot, SnapshotItem]]:
            snap = vol.snapdb.read(snapName)
            for item in snap:
                yield snap, item
        def print_missing_row(row: Tuple[str, str, str]) -> None:
            print(*row, sep="\t")
        print_missing_rows = fmap(print_missing_row)
        get_snap_items_from_snaps = concatMap(get_snap_items_from_snap)
        # get_snap_items_from_snaps = concatMap(vol.snapdb.read)
        missing_count = pipeline(  # TODO broken!
            get_snap_items_from_snaps,
            missing_item_table,
            print_missing_rows,
            count
        )(snapNames)
        if missing_count > 0:
            exitcode = exitcode | 4
    elif args["blob"]:
        if args["path"]:
            for csum in args["<blob>"]:
                csum = ingest(csum)
                print(csum, vol.bs.blob_path(csum).relative_to(cwd))
        elif args["read"]:
            dstFd: BinaryIO
            if args["--output"]:
                dstFd = open(args["--output"], "wb")
            else:
                dstFd = getBytesStdOut()
            for csum in args["<blob>"]:
                with vol.bs.read_handle(csum) as srcFd:
                    copyfileobj(srcFd, dstFd)
            if args["--output"]:
                dstFd.close()  # Only close dstFd if we are writing to a file. stdout shouldn't be closed.
        elif args["type"]:
            for blob in args["<blob>"]:
                blob = ingest(blob)
                print(blob, maybe("unknown", vol.bs.blob_path(blob).filetype()))
        elif args["reverse"]:
            for path in args["<path>"]:
                print(vol.bs.reverser(path))
    elif args["s3"] or args["api"] or args["file"]:
        remote_bs = get_remote_bs(args, cwd)

        def download(blob: str) -> str:
            vol.bs.import_via_fd(lambda: remote_bs.read_handle(blob), blob)
            return blob

        # TODO: upload() is now unused (replaced by session-based loop in the upload branch).
        # Remove once download is also migrated to sessions.
        def upload(blob: str) -> str:
            remote_bs.import_via_fd(lambda: vol.bs.read_handle(blob), blob)
            return blob

        if args["list"]:
            remote_blobs_iter = remote_bs.blobs()
            doer = pipeline(fmap(print), consume)
            doer(remote_blobs_iter)
        elif args["upload"]:
            remote_blobs_iter = remote_bs.blobs()
            remote_blobs = set(
                csum_pbar(label="Fetching remote blobs", quiet=quiet)(
                    remote_blobs_iter
                )
            )
            print(f"Remote Blobs: {len(remote_blobs)}")
            def is_link_item(item: SnapshotItem) -> bool:
                return item.is_link()
            def get_csum(item: SnapshotItem) -> str:
                return item.csum()
            if args["local"]:
                local_blobs_iter = pipeline(
                    ffilter(is_link_item), fmap(get_csum), uniq
                )(iter(vol.tree()))
                local_blobs_pbar: Callable[[Iterable[str]], Generator[str, None, None]] = list_pbar(
                    label="calculating local blobs", quiet=quiet
                )
            elif args["userdata"]:
                local_blobs_iter = vol.bs.blobs()
                local_blobs_pbar = csum_pbar(
                    quiet=quiet, label="calculating local blobs"
                )
            elif args["snap"]:
                snap_name = str(args["<snapshot>"])
                local_blobs_iter = pipeline(
                    ffilter(is_link_item), fmap(get_csum), uniq
                )(iter(vol.snapdb.read(snap_name)))
                local_blobs_pbar = list_pbar(
                    label="calculating local blobs", quiet=quiet
                )
            local_blobs = set(local_blobs_pbar(local_blobs_iter))
            print(f"Local Blobs: {len(local_blobs)}")
            transfer_blobs = local_blobs - remote_blobs
            print(f"Missing Blobs: {len(transfer_blobs)}")
            def blob_postfix(blob: str) -> str:
                return blob
            pb = list_pbar(label="Uploading to remote", quiet=quiet, postfix=blob_postfix)
            with vol.bs.session() as src_sess, remote_bs.session() as dst_sess:
                for blob in pb(transfer_blobs):
                    def get_src(b: str = blob) -> IO[bytes]:
                        return src_sess.read_handle(b)
                    dst_sess.import_via_fd(get_src, blob)
            print(f"Successfully uploaded: {len(transfer_blobs)} Blobs")
        elif args["download"]:
            if args["userdata"]:
                remote_blobs_iter = remote_bs.blobs()
                local_blobs_iter = vol.bs.blobs()
            else:
                raise ValueError("Invalid download source")
            print("Calculating remote blobs")
            remote_blobs = set(
                csum_pbar(label="Calculating remote blobs", quiet=quiet)(
                    remote_blobs_iter
                )
            )
            print(f"Remote Blobs: {len(remote_blobs)}")
            print("Calculating local blobs")
            local_blobs = set(
                csum_pbar(label="calculating local blobs", quiet=quiet)(
                    local_blobs_iter
                )
            )
            print(f"Local Blobs: {len(local_blobs)}")
            transfer_blobs = remote_blobs - local_blobs
            pb = list_pbar(label="Downloading from remote", quiet=quiet)
            print(f"downloading {len(transfer_blobs)} blobs from remote")
            all_success = pipeline(
                pfmaplazy(download, workers=2),
                all
            )(pb(transfer_blobs))
            if all_success:
                print("Successfully downloaded")
            else:
                print("Failed to download")
                exitcode = exitcode | 1
        elif args[
            "check"
        ]:  # TODO what are the check semantics for API? Weird to look at etag.
            if args["s3"]:
                assert isinstance(remote_bs, S3Blobstore)
                def obj_etag(obj: dict) -> str:
                    return obj['ETag'][1:-1]  # Strip quotes from etag, which is how s3 returns it.
                def keep_corrupt(obj: dict) -> bool:
                    return obj_etag(obj) != obj['blob']
                def obj_printr(obj: dict) -> None:
                    print(obj["blob"], obj_etag(obj))
                num_corrupt_blobs = pipeline(
                    blob_stats_progress(label="Checking blobs", quiet=quiet),
                    ffilter(keep_corrupt),
                    fmap(identify(obj_printr)),
                    count,
                )(remote_bs.blob_stats()())  # TODO blob_stats is s3 only.
            elif args["api"] or args["file"]:
                assert isinstance(remote_bs, (HttpBlobstore, FileBlobstore))
                def blob_csum_tuple(blob: str) -> Tuple[str, str]:
                    return blob, remote_bs.blob_checksum(blob)
                @uncurry
                def keep_corrupt_blobs(blob: str, csum: str) -> bool:
                    return blob != csum
                @uncurry
                def corrupt_printr(blob: str, csum: str) -> None:
                    print(blob, csum)
                num_corrupt_blobs = pipeline(
                    csum_pbar(quiet=quiet, label=""),
                    fmap(blob_csum_tuple),
                    ffilter(keep_corrupt_blobs),
                    fmap(identify(corrupt_printr)),
                    count,
                )(remote_bs.blobs())
            if num_corrupt_blobs == 0:
                print("All remote blobs etags match")
            else:
                exitcode = exitcode | 2
        elif args["read"]:
            if args["--output"]:
                dstFd = open(args["--output"], "wb")
            else:
                dstFd = getBytesStdOut()
            for blob in args.get("<blob>"):
                with remote_bs.read_handle(blob) as srcFd:
                    copyfileobj(srcFd, dstFd)
            if args["--output"]:
                dstFd.close()  # Only close dstFd if we are writing to a file. stdout shouldn't be closed.
    elif args["redact"]:
        pattern = args["<pattern>"]
        ignored = [pattern]
        snapName = args["<from>"]
        snap = vol.snapdb.read(snapName)
        is_redacted = ignored_path_checker(ignored)
        def redacted_printr(item: SnapshotItem) -> None:
            print("redacted", item.to_path(vol.root).relative_to(cwd))

        def show_redacted(item: SnapshotItem) -> SnapshotItem:
            if is_redacted(item.to_path(vol.root)):
                redacted_printr(item)
            return item

        is_kept = finvert(is_redacted)
        out_snap = pipeline(fmap(show_redacted), ffilter(is_kept))(iter(snap))
        if args["--noop"]:
            consume(out_snap)
        else:
            vol.snapdb.write(snapName, KeySnapshot(out_snap, snapName, vol.bs.reverser), True)
    return exitcode
