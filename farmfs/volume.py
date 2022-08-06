from errno import ENOENT as NoSuchFile
from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.blobstore import FileBlobstore
from farmfs.util import safetype, partial, ingest, fmap, first, pipeline, ffilter, concat, uniq, jaccard_similarity
from farmfs.fs import Path
from farmfs.fs import ensure_absent, ensure_dir, skip_ignored, ftype_selector, FILE, LINK, DIR, walk
from farmfs.snapshot import TreeSnapshot, KeySnapshot, SnapDelta
from itertools import chain
try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map
try:
    from itertools import ifilter
except ImportError:
    ifilter = filter

def _metadata_path(root):
    assert isinstance(root, Path)
    return root.join(".farmfs")

def _keys_path(root):
    return _metadata_path(root).join("keys")

def _snaps_path(root):
    return _metadata_path(root).join("snaps")

def mkfs(root, udd):
    assert isinstance(root, Path)
    assert isinstance(udd, Path)
    root.mkdir()
    _metadata_path(root).mkdir()
    _keys_path(root).mkdir()
    _snaps_path(root).mkdir()
    kdb = KeyDB(_keys_path(root))
    # Make sure root key is removed.
    kdb.delete("root")
    kdb.write('udd', safetype(udd))
    udd.mkdir()
    kdb.write('status', {})
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
    return list(imap(lambda x: x.get_dict(), snap))

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
        self.bs = FileBlobstore(self.udd)
        self.snapdb = KeyDBFactory(KeyDBWindow("snaps", self.keydb), encode_snapshot, partial(decode_snapshot, self.bs.reverser))
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

    # NOTE: This assumes a posix storage engine.
    def freeze(self, path):
        assert isinstance(path, Path)
        assert isinstance(self.udd, Path)
        csum = path.checksum()
        duplicate = self.bs.import_via_link(path, csum)
        self.bs.link_to_blob(path, csum)
        return {"path": path, "csum": csum, "was_dup": duplicate}

    # Note: This assumes a posix storage engine.
    def thaw(self, user_path):
        assert isinstance(user_path, Path)
        csum_path = user_path.readlink()
        user_path.unlink()
        csum_path.copy(user_path)
        return user_path

    def repair_link(self, path):
        """Find all broken links and point them back at UDD"""
        assert path.islink()
        oldlink = path.readlink()
        if oldlink.isfile():
            return
        csum = self.bs.reverser(oldlink)
        newlink = self.bs.csum_to_path(csum)
        if not newlink.isfile():
            raise ValueError("%s is missing, cannot relink" % newlink)
        else:
            path.unlink()
            self.bs.link_to_blob(path, csum)
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
        snaps = imap(lambda x: self.snapdb.read(x), self.snapdb.list())
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
        blob_op = partial(local_vol.bs.fetch_blob, remote_vol.bs, csum)
        tree_op = partial(local_vol.bs.link_to_blob, path, csum)
        tree_desc = ("Apply mklink %s -> " + delta.csum, path)
        return (blob_op, tree_op, tree_desc)
    else:
        raise ValueError("Unknown mode in SnapDelta: %s" % delta.mode)

# TODO yields lots of SnapDelta. Maybe in wrong file?
def tree_diff(tree, snap):
    tree_parts = iter(tree)
    snap_parts = iter(snap)
    t = None
    s = None
    while True:
        if t is None:
            try:
                t = next(tree_parts)
            except StopIteration:
                pass
        if s is None:
            try:
                s = next(snap_parts)
            except StopIteration:
                pass
        if t is None and s is None:
            return  # We are done!
        elif t is not None and s is not None:
            # We have components from both sides!
            if t < s:
                # The tree component is not present in the snap. Delete it.
                yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                t = None
            elif s < t:
                # The snap component is not part of the tree. Create it
                yield SnapDelta(*s.get_tuple())
                s = None
            elif t == s:
                if t.is_dir() and s.is_dir():
                    pass
                elif t.is_link() and s.is_link():
                    if t.csum() == s.csum():
                        pass
                    else:
                        change = t.get_dict()
                        change['csum'] = s.csum()
                        yield SnapDelta(t._path, t._type, s._csum)
                elif t.is_link() and s.is_dir():
                    yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                    yield SnapDelta(s.pathStr(), SnapDelta.DIR)
                elif t.is_dir() and s.is_link():
                    yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
                    yield SnapDelta(s.pathStr(), SnapDelta.LINK, s.csum())
                else:
                    raise ValueError("Unable to process tree/snap: unexpected types:", s.get_dict()['type'], t.get_dict()['type'])
                s = None
                t = None
            else:
                raise ValueError("Found pair that doesn't respond to > < == cases")
        elif t is not None:
            yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
            t = None
        elif s is not None:
            yield SnapDelta(*s.get_tuple())
            s = None
        else:
            raise ValueError("Encountered case where s t were both not none, but neither of them were none.")
