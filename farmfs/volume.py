from errno import ENOENT as NoSuchFile
from typing import Generator, Optional
from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.blobstore import FileBlobstore, CacheBlobstore
from farmfs.util import safetype, partial, ingest, fmap, first, pipeline, ffilter, concat, uniq, jaccard_similarity
from farmfs.fs import ensure_symlink, Path, ROOT
from farmfs.fs import ensure_absent, ensure_dir, skip_ignored, ftype_selector, FILE, LINK, DIR, walk
from farmfs.snapshot import TreeSnapshot, KeySnapshot, SnapDelta, Snapshot, SnapshotItem
from itertools import chain
import sqlite3

def _metadata_path(root):
    assert isinstance(root, Path)
    return root.join(".farmfs")

def _keys_path(root):
    return _metadata_path(root).join("keys")

def _tmp_path(root):
    return _metadata_path(root).join("tmp")

def _cache_path(root):
    return _metadata_path(root).join("cache")
 

def _db_path(root):
    return _cache_path(root).join("cache.sqlite3")

def _snaps_path(root):
    return _metadata_path(root).join("snaps")

def mkfs(root, udd):
    assert isinstance(root, Path)
    assert isinstance(udd, Path)
    root.mkdir()
    _metadata_path(root).mkdir()
    _keys_path(root).mkdir()
    _snaps_path(root).mkdir()
    _tmp_path(root).mkdir()
    _cache_path(root).mkdir()
    kdb = KeyDB(_keys_path(root))
    # Make sure root key is removed.
    kdb.delete("root")
    # TODO should I overwrite?
    kdb.write('udd', safetype(udd), True)
    udd.mkdir()
    # TODO should I overwrite?
    kdb.write('status', {}, True)
    FarmFSVolume(root)

def directory_signatures(snap, root):
    dirs = {}
    for entry in snap:
        if entry.is_link():
            (path_str, _, csum) = entry.get_tuple()
            parent = root.join(path_str).parent()
            try:
                dirs[parent].update([csum])
            except KeyError:
                dirs[parent] = set([csum])
    return dirs

def encode_volume(vol):
    return safetype(vol.root)

def decode_volume(vol, key):
    return FarmFSVolume(Path(vol))

# TODO duplicated in snapshot
def encode_snapshot(snap):
    return list(map(lambda x: x.get_dict(), snap))

# TODO duplicated in snapshot
def decode_snapshot(reverser, data, key):
    return KeySnapshot(data, key, reverser)

class FarmFSVolume:
    def __init__(self, root):
        assert isinstance(root, Path)
        self.root = root
        self.mdd = _metadata_path(root)
        self.keydb = KeyDB(_keys_path(root))
        self.udd = Path(self.keydb.read('udd'))
        assert self.udd.isdir()
        # TODO Hard coded while bs is known single volume.
        self.tmp_dir = Path(_tmp_path(root))
        assert self.tmp_dir.isdir()
        store = FileBlobstore(self.udd, self.tmp_dir)
        conn = sqlite3.connect(_db_path(root)._path)
        cache = CacheBlobstore(store, conn)
        self.bs = cache
        self.snapdb = KeyDBFactory(KeyDBWindow("snaps", self.keydb), encode_snapshot, partial(decode_snapshot, store.reverser))
        self.remotedb = KeyDBFactory(KeyDBWindow("remotes", self.keydb), encode_volume, decode_volume)
        exclude_file = Path('.farmignore', self.root)
        ignored = [safetype(self.mdd)]
        try:
            with exclude_file.open('rb') as exclude_fd:
                for raw_pattern in exclude_fd.readlines():
                    pattern = ingest(raw_pattern.strip())
                    excluded = safetype(Path(pattern, root))
                    ignored.append(excluded)
        except IOError as e:
            if e.errno == NoSuchFile:
                pass
            else:
                raise e
        self.is_ignored = partial(skip_ignored, ignored)

    def thawed(self, path):
        """Yield set of files not backed by FarmFS under path"""
        get_path = fmap(first)
        select_userdata_files = pipeline(
            ftype_selector([FILE]),
            get_path)
        return select_userdata_files(walk(path, skip=self.is_ignored))

    def frozen(self, path):
        """Yield set of files backed by FarmFS under path"""
        get_path = fmap(first)
        select_userdata_files = pipeline(
            ftype_selector([LINK]),
            get_path)
        return select_userdata_files(walk(path, skip=self.is_ignored))

    def link(self, path, blob):
        """
        Create a path in the volumne bound to a blob in the blobstore.
        This operation is not atomic.
        If path is already a file or directory, those things are destroyed.
        """
        assert isinstance(path, Path)
        assert self.root in path.parents()
        ensure_symlink(path, self.bs.blob_path(blob))

    def freeze(self, path):
        assert isinstance(path, Path)
        assert isinstance(self.udd, Path)
        csum = path.checksum()
        # TODO doesn't work on multi-volume blobstores.
        # TODO we should rework so we try import_via_link then import_via_fd.
        duplicate = self.bs.import_via_link(path, csum)
        # Note ensure_symlink is not atomic, which should be fine for volume.
        self.link(path, csum)
        return {"path": path, "csum": csum, "was_dup": duplicate}

    def thaw(self, user_path):
        assert isinstance(user_path, Path)
        csum_path = user_path.readlink()
        # TODO using bs.tmp_dir. When we allow alternate topology for bs, this will break.
        csum_path.copy_file(user_path, self.tmp_dir)
        return user_path

    def repair_link(self, path):
        """Find all broken links and point them back at UDD"""
        assert path.islink()
        oldlink = path.readlink()
        if oldlink.isfile():
            return
        csum = self.bs.reverser(oldlink)
        newlink = self.bs.blob_path(csum)
        if not newlink.isfile():
            raise ValueError("%s is missing, cannot relink" % newlink)
        else:
            path.unlink()
            path.symlink(self.bs.blob_path(csum))
            return newlink

    def link_checker(self):
        """
        Return a pipeline which given a list of SnapshotItems.
        Returns the SnapshotItems with broken links to the blobstore.
        """
        select_links = ffilter(lambda x: x.is_link())
        is_broken = lambda x: not self.bs.exists(x.csum())
        select_broken = ffilter(is_broken)
        return pipeline(
            select_links,
            select_broken)

    def trees(self):
        """
        Returns an iterator which contains all trees for the volume.
        The Local tree and all the snapshots.
        """
        tree = self.tree()
        snaps = map(lambda x: self.snapdb.read(x), self.snapdb.list())
        return chain([tree], snaps)

    def items(self):
        """Returns an iterator which lists all SnapshotItems from all local snaps + the working tree"""
        return pipeline(
            concat)(self.trees())

    def tree(self):
        """
        Get a snap object which represents the tree of the volume.
        """
        tree_snap = TreeSnapshot(self.root, self.is_ignored, reverser=self.bs.reverser)
        return tree_snap

    def userdata_csums(self):
        """
        Yield all the relative paths (safetype) for all the files in the userdata store.
        """
        # We populate counts with all hash paths from the userdata directory.
        for (path, type_) in walk(self.udd):
            assert isinstance(path, Path)
            if type_ == FILE:
                yield self.bs.reverser(path)
            elif type_ == DIR:
                pass
            else:
                raise ValueError("%s is f invalid type %s" % (path, type_))

    def unused_blobs(self, items):
        """Returns the set of blobs not referenced in items"""
        select_links = ffilter(lambda x: x.is_link())
        get_csums = fmap(lambda item: item.csum())
        referenced_hashes = pipeline(
            select_links,
            get_csums,
            uniq,
            set
        )(items)
        udd_hashes = set(self.userdata_csums())
        missing_data = referenced_hashes - udd_hashes
        assert len(missing_data) == 0, "Missing %s\nReferenced %s\nExisting %s\n" % (missing_data, referenced_hashes, udd_hashes)
        orphaned_csums = udd_hashes - referenced_hashes
        return orphaned_csums

    def similarity(self, dir_a, dir_b):
        """Yields similarity data for directories"""
        get_path = fmap(first)
        get_link = fmap(lambda p: p.readlink())
        get_csum = fmap(self.bs.reverser)
        select_userdata_csums = pipeline(
            ftype_selector([LINK]),
            get_path,
            get_link,
            get_csum,)
        a = set(select_userdata_csums(walk(dir_a, skip=self.is_ignored)))
        b = set(select_userdata_csums(walk(dir_b, skip=self.is_ignored)))
        left = a.difference(b)
        both = a.intersection(b)
        right = b.difference(a)
        jaccard = jaccard_similarity(a, b)
        return (len(left), len(both), len(right), jaccard)

def tree_patcher(local_vol, remote_vol):
    return fmap(partial(tree_patch, local_vol, remote_vol))

def noop():
    pass

def tree_patch(local_vol, remote_vol, delta):
    path = delta.path(local_vol.root)
    assert local_vol.root in path.parents(), "Tried to apply op to %s when root is %s" % (path, local_vol.root)
    if delta.csum is not None:
        csum = delta.csum
    else:
        csum = None
    if delta.mode == delta.REMOVED:
        return (noop, partial(ensure_absent, path), ("Apply Removing %s", path))
    elif delta.mode == delta.DIR:
        return (noop, partial(ensure_dir, path), ("Apply mkdir %s", path))
    elif delta.mode == delta.LINK:
        remote_read_handle_fn = remote_vol.bs.read_handle(csum)
        blob_op = lambda: local_vol.bs.import_via_fd(remote_read_handle_fn, csum)
        tree_op = lambda: ensure_symlink(path, local_vol.bs.blob_path(csum))
        tree_desc = ("Apply mklink %s -> " + delta.csum, path)
        return (blob_op, tree_op, tree_desc)
    else:
        raise ValueError("Unknown mode in SnapDelta: %s" % delta.mode)


def next_valid_snap_item(
        item_iter: Generator[SnapshotItem, None,None],
        last_delta: Optional[SnapDelta]
        ) -> Optional[SnapshotItem]:
    """
    Get the next valid snapshot item from the iterator.
    A valid snapshot item is one which does not belong to
    a parent which was deleted in the last delta.
    """
    if last_delta is None:
        # There could not be a invalid item, because there have been no deltas.
        return next(item_iter, None)
    if last_delta.mode != SnapDelta.REMOVED:
        # If the last delta was not a removal, we can safely return the next item.
        return next(item_iter, None)
    # We last processed a REMOVE, lets comsume children if it was a dir.
    removed_path = last_delta.path(ROOT)
    while True:
        next_item = next(item_iter, None)
        if next_item is None:
            return None
        item_path = next_item.to_path(ROOT)
        if removed_path in item_path.parents():
            continue
        else:
            return next_item

# TODO yields lots of SnapDelta. Maybe in wrong file?
def tree_diff(tree: Snapshot, snap: Snapshot):
    tree_parts = iter(tree)
    snap_parts = iter(snap)
    t = next(tree_parts, None)
    s = next(snap_parts, None)
    while t is not None or s is not None:
        if t is not None and s is not None:
            # We have components from both sides!
            if t < s:
                # The tree component is not present in the snap. Delete it.
                sd = SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                t = next_valid_snap_item(tree_parts, sd)
                yield sd
            elif s < t:
                # The snap component is not part of the tree. Create it
                yield SnapDelta(*s.get_tuple())
                s = next(snap_parts, None)
            elif t == s:
                if t.is_dir() and s.is_dir():
                    t = next(tree_parts, None)
                    s = next(snap_parts, None)
                elif t.is_link() and s.is_link():
                    if t.csum() == s.csum():
                        t = next(tree_parts, None)
                        s = next(snap_parts, None)
                    else:
                        change = t.get_dict()
                        change['csum'] = s.csum()
                        sd = SnapDelta(t._path, t._type, s._csum)
                        t = next_valid_snap_item(tree_parts, sd)
                        s = next(snap_parts, None)
                        yield sd
                elif t.is_link() and s.is_dir():
                    sd = SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                    t = next_valid_snap_item(tree_parts, sd)
                    yield sd
                    yield SnapDelta(s.pathStr(), SnapDelta.DIR)
                    s = next(snap_parts, None)
                elif t.is_dir() and s.is_link():
                    yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                    yield SnapDelta(s.pathStr(), SnapDelta.LINK, s.csum())
                    t = next(tree_parts, None)
                    s = next(snap_parts, None)
                else:
                    raise ValueError("Unable to process tree/snap: unexpected types:", s.get_dict()['type'], t.get_dict()['type'])
            else:
                raise ValueError("Found pair that doesn't respond to > < == cases")
        elif t is not None:
            sd = SnapDelta(t.pathStr(), SnapDelta.REMOVED)
            t = next_valid_snap_item(tree_parts, sd)
            yield sd
        elif s is not None:
            yield SnapDelta(*s.get_tuple())
            s = next(snap_parts, None)
        else:
            raise ValueError("Encountered case where s t were both not none, but neither of them were none.")
