from os import mkdir
from os import listdir
from os import link
from os import unlink
from os import symlink
from os import readlink
from os import rmdir
from os import stat
from os import chmod
from os import rename
from os import lstat
from errno import ENOENT as FileDoesNotExist
from errno import EEXIST as FileExists
from errno import EISDIR as DirectoryExists
from errno import EINVAL as InvalidArgument
from errno import EPERM as NotPermitted
from errno import EISDIR as IsADirectory
from hashlib import md5
from os.path import exists
from os.path import isabs
from os.path import isdir
from os.path import isfile, islink, sep
from os.path import normpath
from os.path import split
from os.path import stat as statc
from os.path import splitext
from shutil import copyfileobj
from fnmatch import fnmatchcase
from functools import total_ordering
from farmfs.util import ingest, safetype, uncurry, first, ffilter
from future.utils import python_2_unicode_compatible
from safeoutput import open as safeopen
from safeoutput import _sameDir as sameDir
from filetype import guess, Type
import filetype

ERRORS = [
    FileDoesNotExist,
    FileExists,
    DirectoryExists,
    InvalidArgument,
    NotPermitted,
    IsADirectory]

class XSym(Type):
    '''Implements OSX XSym link file type detector'''
    def __init__(self):
        super(XSym, self).__init__(
            mime='inode/symlink',
            extension='xsym')

    def match(self, buf):
        """
        Detects the MS-Dos symbolic link format from OSX.
        Format of XSym files taken from section 11.7.3 of Mac OSX Internals
        """
        X = 0x58
        S = 0x53
        Y = 0x79
        M = 0x6d
        NEWLINE = 0xa
        ZERO = 0x30
        NINE = 0x39
        return len(buf) >= 10 and \
            buf[0] == X and \
            buf[1] == S and \
            buf[2] == Y and \
            buf[3] == M and \
            buf[4] == NEWLINE and \
            buf[5] >= ZERO and buf[5] <= NINE and \
            buf[6] >= ZERO and buf[6] <= NINE and \
            buf[7] >= ZERO and buf[7] <= NINE and \
            buf[8] >= ZERO and buf[8] <= NINE and \
            buf[9] == NEWLINE


# XXX Dirty, we are touching the set of types in filetype package.
filetype.types.append(XSym())

_BLOCKSIZE = 65536

LINK = u'link'
FILE = u'file'
DIR = u'dir'

TYPES = [LINK, FILE, DIR]

# TODO should take 1 arg, return fn.
def skip_ignored(ignored, path, ftype=None):
    for i in ignored:
        if fnmatchcase(path._path, i):
            return True
    else:
        return False

def ftype_selector(keep_types):
    keep = lambda p, ft: ft in keep_types  # Take p and ft since we may want to use it in entries.
    return ffilter(uncurry(keep))

@total_ordering
@python_2_unicode_compatible
class Path:
    def __init__(self, path, frame=None, fast=False):
        # output = Path( self._path + sep + child)
        if fast:
            # Fast path is generated by walk. frame is already a Path and path is a single element from listdir.
            self._path = frame._path + sep + path
            self._parent = frame
        elif isinstance(path, Path):
            # Copy constructor from another Path.
            assert frame is None
            self._path = path._path
            self._parent = path._parent
        else:
            if path is None:
                raise ValueError("path must be defined")
            path = ingest(path)
            if frame is None:
                self._path = normpath(path)
                assert self._path.startswith(sep), "Frame is required when building relative paths: %s" % path
            else:
                assert isinstance(frame, Path)
                assert not path.startswith(sep), "path %s is required to be relative when a frame %s is provided" % (path, frame)
                self._path = normpath(frame._path + sep + path)
            self._parent = None
        assert isinstance(self._path, safetype)

    def __str__(self):
        return self._path

    def __repr__(self):
        return str(self)

    def mkdir(self):
        try:
            mkdir(self._path)
        except OSError as e:
            if e.errno == FileExists:
                pass
            elif e.errno == DirectoryExists:
                pass
            else:
                raise e

    def parent(self):
        """
        Returns the parent of self. If self is root ('/'), parent returns None.
        You much check the output of parent before using the value.
        Notcie that parent of root in the shell is '/', so this is a semantic difference
        between FarmFS and POSIX.
        """
        if self._path == sep:
            return None
        elif self._parent is None:
            self._parent = Path(first(split(self._path)))
            return self._parent
        else:
            return self._parent

    def parents(self):
        # TODO turn into comprehension.
        paths = [self]
        path = self
        parent = path.parent()
        while parent is not None:
            paths.append(parent)
            path = parent
            parent = path.parent()
        return reversed(paths)

    def name(self):
        return second(split(self._path))

    def extension(self):
        root, ext = splitext(self._path)
        if ext == '':
            return None
        return ext

    def relative_to(self, frame):
        assert isinstance(frame, Path)
        # Fast mode check for normalized path decendents.
        if len(self._path) >= len(frame._path) + 2 and \
            self._path.startswith(frame._path) and \
                self._path[len(frame._path) + 1] == sep:
            return self._path[len(frame._path):]
        # Get the segment sequences from root to self and frame.
        self_family = iter(self.parents())
        frame_family = iter(frame.parents())
        # Find the common ancesstor of self and frame.
        s = None
        f = None
        common = None
        while True:
            s = next(self_family, None)
            f = next(frame_family, None)
            if s is None and f is None:
                if common is None:
                    # common should have at least advanced to root!
                    raise ValueError("Failed to find common decendent of %s and %s" % (self, frame))
                else:
                    # self and frame exhaused at the same time. Must be the same path.
                    return SELF_STR
            elif s is None:
                # frame is a decendent of self. Self is an ancesstor of frame.
                # We can return remaining segments of frame.
                # Self is "/a" frame = "/a/b/c" common is "/a" result is "../.."
                backtracks = len(list(frame_family)) + 1
                backtrack = [PARENT_STR] * backtracks
                backtrack = sep.join([PARENT_STR] * backtracks)
                # raise NotImplementedError("self %s frame %s common %s backtracks %s backtrack %s" % (
                #    self, frame, common, backtracks, backtrack))
                return backtrack
            elif f is None:
                # self is a decendent of frame. frame is an ancesstor of self.
                # We can return remaining segments of self.
                if common == ROOT:
                    return self._path[len(common._path):]
                else:
                    return self._path[len(common._path) + 1:]
            elif s == f:
                # self and frame decendent are the same, so advance.
                common = s
                pass
            else:
                # we need to backtrack from frame to common.
                backtracks = len(list(frame_family)) + 1
                backtrack = [PARENT_STR] * backtracks
                backtrack = sep.join([PARENT_STR] * backtracks)
                if common == ROOT:
                    forward = self._path[len(common._path):]
                else:
                    forward = self._path[len(common._path) + 1:]
                # print("backtracks", backtracks, "backtrack", backtrack, "forward", forward, "common", common)
                return backtrack + sep + forward

    def exists(self):
        """Returns true if a path exists. This includes symlinks even if they are broken."""
        return self.islink() or exists(self._path)

    def readlink(self, frame=None):
        """
        Returns the link destination if the Path is a symlink.
        If the path doesn't exist, raises FileNotFoundError
        If the path is not a symlink raises OSError Errno InvalidArgument.
        """
        return Path(readlink(self._path), frame)

    def link(self, dst):
        """
        Creates a hard link to dst.
              dst
              DNE Dir F   SLF SLD SLB
        s DNR  R   R   N   N   R   R
        e Dir  R   R   R   R   R   R
        l F    R   R   R   R   ?   ?
        f SL   R   R   R   R   ?   ?
        R means raises.
        N means new hardlink created.
        """
        assert isinstance(dst, Path)
        link(dst._path, self._path)

    def symlink(self, dst):
        assert isinstance(dst, Path)
        symlink(dst._path, self._path)

    def copy_fd(self, src_fd, tmpdir=None):
        """
        Reads src_fd and puts the contents into a file located at self._path.
        """
        if tmpdir is None:
            tmpfn = sameDir
        else:
            tmpfn = lambda _: tmpdir._path
        with safeopen(self._path, 'wb', useDir=tmpfn) as dst_fd:
            copyfileobj(src_fd, dst_fd)

    # TODO this behavior is the opposite of what one would expect.
    def copy_file(self, dst, tmpdir=None):
        """
        Copy self to path dst.
        Does not attempt to ensure dst is a valid destination.
        Raises IsADirectoryError and FileDoesNotExist on namespace errors.
        The file will either be fully copied, or will not be created.
        This is achieved via temp files and atomic swap.
        This API works for large files, as data is read in chunks and sent
        to the destination.
        """
        if tmpdir is None:
            tmpfn = sameDir
        else:
            tmpfn = lambda _: tmpdir._path
        assert isinstance(dst, Path)
        with open(self._path, 'rb') as src_fd:
            with safeopen(dst._path, 'wb', useDir=tmpfn) as dst_fd:
                copyfileobj(src_fd, dst_fd)

    def unlink(self, clean=None):
        try:
            unlink(self._path)
        except OSError as e:
            if e.errno == FileDoesNotExist:
                pass
            else:
                raise e
        if clean is not None:
            parent = self.parent()
            parent._cleanup(clean)

    def rmdir(self, clean=None):
        rmdir(self._path)
        if clean is not None:
            parent = self.parent()
            parent._cleanup(clean)

    """Called on the parent of file or directory after a removal
    (if cleanup as asked for). Recuses cleanup until it reaches terminus.
    """
    def _cleanup(self, terminus):
        assert isinstance(terminus, Path)
        assert terminus in self.parents()
        if self == terminus:
            return
        if len(self.dir_list()) == 0:
            self.rmdir(terminus)

    def islink(self):
        return islink(self._path)

    def isdir(self):
        return isdir(self._path)

    def isfile(self):
        return isfile(self._path)

    def checksum(self):
        """
        If self path is a file or a symlink to a file, compute a checksum returned as a string.
        If self points to a missing file or a broken symlink, raises FileDoesNotExist.
        If self points to a directory or a symlink facing directory, raises IsADirectory.
        """
        hasher = md5()
        with self.open('rb') as fd:
            buf = fd.read(_BLOCKSIZE)
            while len(buf) > 0:
                # TODO Could cancel work here.
                hasher.update(buf)
                buf = fd.read(_BLOCKSIZE)
            digest = safetype(hasher.hexdigest())
            return digest

    def __cmp__(self, other):
        return (self > other) - (self < other)

    def __eq__(self, other):
        assert isinstance(other, Path)
        return self._path == other._path

    def __ne__(self, other):
        assert isinstance(other, Path)
        return not (self == other)

    def __lt__(self, other):
        assert isinstance(other, Path)
        return self._path.split(sep) < other._path.split(sep)

    def __hash__(self):
        return hash(self._path)

    def join(self, child):
        child = safetype(child)
        try:
            output = Path(self._path + sep + child)
        except UnicodeDecodeError as e:
            raise ValueError(str(e) + "\nself path: " + self._path + "\nchild: ", child)
        return output

    def dir_list(self):
        names = sorted(listdir(self._path))
        paths = [Path(n, self, fast=True) for n in names]
        return paths

    def ftype(self):
        st = lstat(self._path)
        if statc.S_ISLNK(st.st_mode):
            return LINK
        elif statc.S_ISREG(st.st_mode):
            return FILE
        elif statc.S_ISDIR(st.st_mode):
            return DIR
        else:
            raise ValueError("%s is not in %s" % (self, TYPES))

    def open(self, mode):
        return open(self._path, mode)

    def stat(self):
        return stat(self._path)

    def chmod(self, mode):
        return chmod(self._path, mode)

    def rename(self, dst):
        return rename(self._path, dst._path)

    def filetype(self):
        # XXX Working around bug in filetype guess.
        # Duck typing checks don't work on py27, because of str bytes confusion.
        # So we read the file outselves and put it in a bytearray.
        # Remove this when we drop support for py27.
        with self.open("rb") as fd:
            type = guess(bytearray(fd.read(256)))
            if type:
                return type.mime
            else:
                return None

def userPath2Path(arg, frame):
    """
    Building paths using conventional POSIX systems will discard CWD if the
    path is absolute. FarmFS makes passing of CWD explicit so that path APIs
    are pure functions. Additionally FarmFS path construction doesn't allow
    for absolute paths to be mixed with frames. This is useful for
    spotting bugs and making sure that pathing has strong guarantees. However
    this comes at the expense of user expectation. When dealing with user
    input, there is an expecation that POSIX semantics are at play.
    userPath2Path checks to see if the provided path is absolute, and if not,
    adds the CWD frame.
    """
    arg = ingest(arg)
    if isabs(arg):
        return Path(arg)
    else:
        return Path(arg, frame)

# TODO this function is dangerous. Would be better if we did sorting in the snaps to ensure order of ops explicitly.
def ensure_absent(path):
    if path.exists():
        if path.isdir():
            for child in path.dir_list():
                ensure_absent(child)
            path.rmdir()
        else:
            path.unlink()
    else:
        pass  # No work to do.

def ensure_dir(path):
    if path.exists():
        if path.isdir():
            pass  # There is nothing to do.
        else:
            path.unlink()
            path.mkdir()
    else:
        assert path != ROOT, "Path is root, which must be a directory"
        parent = path.parent()
        assert parent != path, "Path and parent were the same!"
        ensure_dir(parent)
        path.mkdir()

def ensure_link(path, orig):
    assert orig.exists()
    parent = path.parent()
    assert parent != path, "Path and parent were the same!"
    ensure_dir(parent)
    ensure_absent(path)
    path.link(orig)


write_mask = statc.S_IWUSR | statc.S_IWGRP | statc.S_IWOTH
read_only_mask = ~write_mask

def ensure_readonly(path):
    mode = path.stat().st_mode
    read_only = mode & read_only_mask
    path.chmod(read_only)

# TODO this is used only for fsck readonly check.
def is_readonly(path):
    mode = path.stat().st_mode
    writable = mode & write_mask
    return bool(writable)

def ensure_copy(dst, src, tmpdir=None):
    assert src.exists()
    parent = dst.parent()
    assert parent != dst, "dst and parent were the same!"
    ensure_dir(parent)
    ensure_absent(dst)
    src.copy_file(dst, tmpdir)

def ensure_rename(path, orig):
    assert orig.exists()
    parent = path.parent()
    assert parent != path, "Path and parent were the same!"
    ensure_dir(parent)
    ensure_absent(path)
    orig.rename(path)

def ensure_symlink(path, target):
    ensure_symlink_unsafe(path, target._path)

def ensure_symlink_unsafe(path, orig):
    parent = path.parent()
    assert parent != path, "Path and parent were the same!"
    ensure_dir(parent)
    ensure_absent(path)
    assert not path.exists()
    symlink(orig, path._path)
    assert path.islink()


def ensure_file(path, mode):
    """
    Creates/Deletes directories. Does whatever is required inorder
    to make and open a file with the mode previded.

    Mode settings to consider are:
     O_CREAT         create file if it does not exist
     O_TRUNC         truncate size to 0
     O_EXCL          error if O_CREAT and the file exists
    """
    assert isinstance(path, Path)
    parent = path.parent()
    assert parent != path, "Path and parent were the same!"
    ensure_dir(parent)
    fd = path.open(mode)
    return fd


ROOT = Path(sep)
PARENT_STR = safetype("..")
SELF_STR = safetype(".")

def walk(*roots, **kwargs):
    skip = kwargs.get('skip', None)
    dirs = [iter(sorted(roots))]
    while len(dirs) > 0:
        curDir = dirs[-1]
        curPath = next(curDir, None)
        if curPath is None:
            dirs.pop()
        else:
            type = curPath.ftype()
            if skip and skip(curPath, type):
                continue
            yield (curPath, type)
            if type is DIR:
                children = curPath.dir_list()
                dirs.append(iter(children))
