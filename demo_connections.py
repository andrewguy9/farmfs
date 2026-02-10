from farmfs.fs import Path
from farmfs import cwd
from farmfs.util import pipeline, ffilter
from hashlib import md5
from typing import BinaryIO
from typing import Callable, Generator, Iterator, Literal, IO

def list_dir(path: Path) -> Generator[Path, None, None]:
    for item in path.dir_list():
        print("Found path: %s" % item)
        yield item

Mode = Literal['r', 'w', 'br', 'bw']
HandleThunk = Callable[[], IO]
def handle_thunk(path: Path, mode: Mode) -> HandleThunk:
    print("Creating thunk for %s with mode %s" % (path, mode))
    def thunk():
        print("Opening %s with mode %s" % (path, mode))
        return path.open(mode)
    return thunk

def io_factory(mode: Mode):
    def file_pipeline(paths: Iterator[Path]) -> Generator[HandleThunk, None, None]:
        for path in paths:
            thunk = handle_thunk(path, mode)
            yield thunk
    return file_pipeline

def checksum_pipeline(items: Iterator[HandleThunk]) -> Generator[str, None, None]:
    for thunk in items:
        with thunk() as f:
            print("Computing checksum for %s" % f.name)
            hasher = md5()
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)
            yield hasher.hexdigest()

def unique_pipeline(items: Iterator[str]) -> Generator[str, None, None]:
    seen = set()
    for item in items:
        if item not in seen:
            print("New value: %s" % item)
            yield item
            seen.add(item)
        else:
            print("Duplicate value: %s" % item)

def files_only(paths: Iterator[Path]) -> Generator[Path, None, None]:
    for path in paths:
        if path.isfile():
            print("Yielding file: %s" % path)
            yield path
        else:
            print("Skipping non-file: %s" % path)

# For each input directory path, list the files, open them, compute their checksums and return the unique checksums.
checksum_dir_pipeline = pipeline(
    list_dir,
    files_only,
    io_factory('br'),
    checksum_pipeline,
    unique_pipeline)
checksums = checksum_dir_pipeline(cwd)
print("Consuming checksums...")
for checksum in checksums:
    print("output checksum: %s" % checksum)
print("Done.")
