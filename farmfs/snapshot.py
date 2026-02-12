from delnone import delnone
from farmfs.blobstore import ReverserFunction
from farmfs.fs import Path, LINK, DIR, FILE, SkipFunction, ingest, ROOT, walk
from functools import total_ordering
from os.path import sep
from typing import Any, Dict, Generator, List, Optional, Tuple, Union
from typing import overload


@total_ordering
class SnapshotItem:
    def __init__(self, path: Path | str, type: str, csum: str | None = None):
        assert isinstance(type, str)
        assert type in [LINK, DIR], type
        if isinstance(path, Path):
            path = path._path  # TODO reaching into path.
        assert isinstance(path, str), path
        if type == LINK:
            if csum is None:
                raise ValueError("checksum should be specified for links")
        self._path = ingest(path)  # TODO do we know this is already str?
        self._type = ingest(type)
        self._csum = csum and ingest(csum)  # csum can be None.

    # TODO create a path comparator. cmp has different semantics.
    def __cmp__(self, other: Any) -> int:
        if other is None:
            return -1
        if not isinstance(other, SnapshotItem):
            return NotImplemented
        # Legacy snaps have leading '/' and modern ones are realative to ROOT.
        # Adding a './' before allows us to work around th issue.
        self_path = Path("./" + self._path, ROOT)
        other_path = Path("./" + other._path, ROOT)
        return self_path.__cmp__(other_path)

    def __eq__(self, other: Any) -> bool:
        return self.__cmp__(other) == 0

    def __ne__(self, other: Any) -> bool:
        return self.__cmp__(other) != 0

    def __lt__(self, other: Any) -> bool:
        return self.__cmp__(other) < 0

    def get_tuple(self) -> Tuple[str, str, str | None]:
        return (self._path, self._type, self._csum)

    # TODO we should specify what keys/values are in the dict.
    def get_dict(self) -> dict:
        return delnone(dict(path=self._path, type=self._type, csum=self._csum))

    def pathStr(self) -> str:
        assert isinstance(self._path, str)
        return self._path

    def is_dir(self) -> bool:
        return self._type == DIR

    def is_link(self) -> bool:
        return self._type == LINK

    def csum(self) -> str:
        # TODO assert type isn't a great exception for this.
        assert self._type == LINK, (
            "Encountered unexpected type %s in SnapshotItem for path %s"
            % (self._type, self._path)
        )
        assert self._csum is not None
        return self._csum

    def __str__(self):
        return "<%s %s %s>" % (self._type, self._path, self._csum)

    def to_path(self, root: Path) -> Path:
        return root.join(self._path)


class Snapshot:
    name: str

    def __init__(self, name: str):
        self.name = name

    def __iter__(self) -> Generator[SnapshotItem, None, None]:
        raise NotImplementedError()


class TreeSnapshot(Snapshot):
    def __init__(self, root: Path, is_ignored: SkipFunction, reverser: ReverserFunction):
        super().__init__("<tree>")
        assert isinstance(root, Path)
        self.root = root
        self.is_ignored = is_ignored
        self.reverser = reverser

    def __iter__(self) -> Generator[SnapshotItem, None, None]:
        root = self.root

        def tree_snap_iterator() -> Generator[SnapshotItem, None, None]:
            for path, type_ in walk(root, skip=self.is_ignored):
                if type_ is LINK:
                    # We put the link destination through the reverser.
                    # We don't control the link, so its possible the value is
                    # corrupt, like say wrong volume.
                    # Or perhaps crafted to cause problems.
                     #TODO we are doign str -> Path -> str pointlessly.
                    ud_str = self.reverser(str(path.readlink()))
                elif type_ is DIR:
                    ud_str = None
                elif type_ is FILE:
                    continue
                else:
                    raise ValueError(
                        "Encounted unexpected type %s for path %s" % (type_, path)
                    )
                yield SnapshotItem(path.relative_to(root), type_, ud_str)

        return tree_snap_iterator()


# TODO this is a lame way of describing whats in the snaps.
SnapItemTypes = Union[List, Dict]
class KeySnapshot(Snapshot):
    def __init__(self, data: List[SnapItemTypes], name: str, reverser: ReverserFunction):
        super().__init__(name)
        assert data is not None
        self.data = data
        self._reverser = reverser

    def __iter__(self):
        def key_snap_iterator():
            assert self.data
            for item in self.data:
                if isinstance(item, list):
                    assert len(item) == 3
                    (path_str, type_, ref) = item
                    assert isinstance(path_str, str)
                    assert isinstance(type_, str)
                    if ref is not None:
                        csum = self._reverser(ref)
                        assert isinstance(csum, str)
                    else:
                        csum = None
                    parsed = SnapshotItem(path_str, type_, csum)
                elif isinstance(item, dict):
                    parsed = SnapshotItem(**item)
                yield parsed

        return iter(sorted(key_snap_iterator()))


class SnapDelta:
    REMOVED = "removed"
    DIR = DIR
    LINK = LINK
    # TODO modes could be a literal type.
    _modes = [REMOVED, DIR, LINK]

    # TODO mode could be a literal type.
    def __init__(self, pathStr: str, mode: str, csum: Optional[str] = None):
        assert isinstance(pathStr, str), "didn't expect type %s" % type(pathStr)
        assert isinstance(mode, str) and mode in self._modes
        if mode == self.LINK:
            # Make sure that we are looking at a csum, not a path.
            assert csum is not None and csum.count(sep) == 0
        else:
            assert csum is None
        self._pathStr = pathStr
        self.mode = mode
        self.csum = csum

    # TODO this function worked as str and as path, which is it?
    @overload
    def path(self, root: Path) -> Path: ...
    @overload
    def path(self, root: str) -> str: ...
    def path(self, root: Union[Path, str]) -> Union[Path, str]:
        if isinstance(root, Path):
            return root.join(self._pathStr)
        return root.join(self._pathStr)

    def __str__(self) -> str:
        # TODO Not a great encoding.
        return "{" + self.path("") + "," + self.mode + "," + self.csum + "}"

    def __repr__(self):
        return f'SnapDelta("{self._pathStr}", {self.mode}, {self.csum})'