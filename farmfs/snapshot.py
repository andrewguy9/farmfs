from farmfs.fs import Path, LINK, DIR, FILE, ingest, ROOT, walk
from delnone import delnone
from os.path import sep
from functools import total_ordering
from farmfs.util import safetype
from future.utils import python_2_unicode_compatible

try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map

@total_ordering
@python_2_unicode_compatible
class SnapshotItem:
    def __init__(self, path, type, csum=None):
        assert isinstance(type, safetype)
        assert type in [LINK, DIR], type
        if (isinstance(path, Path)):
            path = path._path  # TODO reaching into path.
        assert isinstance(path, safetype), path
        if type == LINK:
            if csum is None:
                raise ValueError("checksum should be specified for links")
        self._path = ingest(path)  # TODO do we know this is already safetype?
        self._type = ingest(type)
        self._csum = csum and ingest(csum)  # csum can be None.

    # TODO create a path comparator. cmp has different semantics.
    def __cmp__(self, other):
        assert other is None or isinstance(other, SnapshotItem)
        if other is None:
            return -1
        # Legacy snaps have leading '/' and modern ones are realative to ROOT.
        # Adding a './' before allows us to work around th issue.
        self_path = Path("./" + self._path, ROOT)
        other_path = Path("./" + other._path, ROOT)
        return self_path.__cmp__(other_path)

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def get_tuple(self):
        return (self._path, self._type, self._csum)

    def get_dict(self):
        return delnone(dict(path=self._path,
                            type=self._type,
                            csum=self._csum))

    def pathStr(self):
        assert isinstance(self._path, safetype)
        return self._path

    def is_dir(self):
        return self._type == DIR

    def is_link(self):
        return self._type == LINK

    def csum(self):
        assert self._type == LINK, "Encountered unexpected type %s in SnapshotItem for path %s" % (self._type, self._path)
        return self._csum

    def __str__(self):
        return "<%s %s %s>" % (self._type, self._path, self._csum)

    def to_path(self, root):
        return root.join(self._path)

class Snapshot:
    pass

class TreeSnapshot(Snapshot):
    def __init__(self, root, is_ignored, reverser):
        assert isinstance(root, Path)
        self.root = root
        self.is_ignored = is_ignored
        self.reverser = reverser
        self.name = '<tree>'

    def __iter__(self):
        root = self.root
        def tree_snap_iterator():
            for path, type_ in walk(root, skip=self.is_ignored):
                if type_ is LINK:
                    # We put the link destination through the reverser.
                    # We don't control the link, so its possible the value is
                    # corrupt, like say wrong volume.
                    # Or perhaps crafted to cause problems.
                    ud_str = self.reverser(path.readlink())
                elif type_ is DIR:
                    ud_str = None
                elif type_ is FILE:
                    continue
                else:
                    raise ValueError("Encounted unexpected type %s for path %s" % (type_, path))
                yield SnapshotItem(path.relative_to(root), type_, ud_str)
        return tree_snap_iterator()

class KeySnapshot(Snapshot):
    def __init__(self, data, name, reverser):
        assert data is not None
        self.data = data
        self._reverser = reverser
        self.name = name

    def __iter__(self):
        def key_snap_iterator():
            assert self.data
            for item in self.data:
                if isinstance(item, list):
                    assert len(item) == 3
                    (path_str, type_, ref) = item
                    assert isinstance(path_str, safetype)
                    assert isinstance(type_, safetype)
                    if ref is not None:
                        csum = self._reverser(ref)
                        assert isinstance(csum, safetype)
                    else:
                        csum = None
                    parsed = SnapshotItem(path_str, type_, csum)
                elif isinstance(item, dict):
                    parsed = SnapshotItem(**item)
                yield parsed
        return iter(sorted(key_snap_iterator()))

@python_2_unicode_compatible
class SnapDelta:
    REMOVED = u'removed'
    DIR = DIR
    LINK = LINK
    _modes = [REMOVED, DIR, LINK]

    def __init__(self, pathStr, mode, csum=None):
        assert isinstance(pathStr, safetype), "didn't expect type %s" % type(pathStr)
        assert isinstance(mode, safetype) and mode in self._modes
        if mode == self.LINK:
            # Make sure that we are looking at a csum, not a path.
            assert csum is not None and csum.count(sep) == 0
        else:
            assert csum is None
        self._pathStr = pathStr
        self.mode = mode
        self.csum = csum

    def path(self, root):
        return root.join(self._pathStr)

    def __str__(self):
        # TODO Not a great encoding.
        return "{" + self.path("") + "," + self.mode + "," + self.csum + "}"

# TODO duplicated in volume
def encode_snapshot(snap):
    return list(imap(lambda x: x.get_dict(), snap))

# TODO duplicated in volume
def decode_snapshot(splitter, reverser, data, key):
    return KeySnapshot(data, key, splitter, reverser)
