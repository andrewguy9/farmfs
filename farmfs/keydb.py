from collections.abc import Callable
from typing import Any, Iterable, List, Optional, Protocol, Tuple, runtime_checkable
from farmfs.blobstore import FileBlobstore
from farmfs.fs import Path, ensure_symlink
from farmfs.fs import walk
from hashlib import md5
from json import loads, JSONEncoder
from errno import ENOENT as NoSuchFile
from errno import EISDIR as IsDirectory
from os.path import sep
from farmfs.util import egest
from io import BytesIO

keydb_encoder = JSONEncoder(ensure_ascii=False, sort_keys=True)


@runtime_checkable
class KeyDBLike(Protocol):
    def write(self, key: str, value: Any, overwrite: bool) -> None: ...
    def read(self, key: str) -> Any: ...
    def key_blob(self, key: str) -> Optional[str]: ...
    def list(self) -> List[str]: ...
    def delete(self, key: str) -> None: ...


def checksum(value_bytes: bytes) -> str:
    """
    Input string should already be coersed into an encoding before being
    provided
    """
    return md5(value_bytes).hexdigest()


class KeyDB:
    def __init__(self, db_path: Path, tmp_dir: Path, blobstore: FileBlobstore | None = None):
        assert isinstance(db_path, Path)
        self.root = db_path
        self.tmp_dir = tmp_dir
        self.bs = blobstore

    def keypath(self, key: str) -> Path:
        key = str(key)
        return self.root.join(key)

    def _readparts_file(self, key_path: Path) -> Optional[Tuple[bytes, str]]:
        """
        If a key is file backed, read both the file and its checksum from the store.
        Read the raw bytes and verification checksum from a key file.
        Returns None if the key does not exist.
        Does not validate the key.
        """
        try:
            with key_path.open("rb") as f:
                obj_bytes = f.readline().strip()
                verify_checksum = f.readline().strip()
                return obj_bytes, verify_checksum.decode("utf-8")
        except IOError as e:
            if e.errno == NoSuchFile or e.errno == IsDirectory:
                return None
            else:
                raise e

    def _is_blob(self, key_path: Path) -> bool:
        """
        True if key_path is a symlink.
        False if it doesn't exist or if it's a regular file or directory.
        """
        return key_path.islink()

    def _read_bytes_blob(self, key_path: Path) -> Optional[bytes]:
        """
        Read the bytes from a blob backed key.
        """
        try:
            with key_path.open("rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def _read_csum_blob(self, key_path: Path) -> Optional[str]:
        """
        Read the checksum from a blob backed key.
        """
        if self.bs is None:
            raise ValueError("Blobstore is required to verify blob-backed keys")
        try:
            link_path = key_path.readlink()
            blob = self.bs.reverser(link_path)
            return blob
        except FileNotFoundError:
            return None

    # TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME
    # ITERATOR BASED RECORD TYPE.
    def write(self, key: str, value: Any, overwrite: bool) -> None:
        key = str(key)
        key_path = self.keypath(key)
        if key_path.exists() and not overwrite:
            raise ValueError("Key %s already exists" % key)
        value_str = keydb_encoder.encode(value)
        value_bytes = egest(value_str)
        value_hash = checksum(value_bytes)
        if self.bs is None:
            raise ValueError("Blobstore is required to write blob-backed keys")
        self.bs.import_via_fd(lambda: BytesIO(value_bytes), value_hash)
        blob_path = self.bs.blob_path(value_hash)
        ensure_symlink(key_path, blob_path)


    def key_blob(self, key: str) -> Optional[str]:
        """Return the stored checksum for a key, or None if the key does not exist."""
        key_path = self.keypath(key)
        if self._is_blob(key_path):
            csum = self._read_csum_blob(key_path)
        else:  # File based key
            parts = self._readparts_file(key_path)
            if parts is None:
                return None
            _, csum = parts
        return csum

    def _read_raw(self, key_path: Path) -> bytes | None:
        if self._is_blob(key_path):
            data = self._read_bytes_blob(key_path)
            if data is None:
                return None
        else:  # file
            parts = self._readparts_file(key_path)
            if parts is None:
                return None
            data, _ = parts
        return data

    # TODO semantics of read are bad. None could be json Null or it could be absent key.
    # TODO Why is this JSON default out? BasicKeydb should return bytes and a JSON KeyDB should be JSON Any.
    def read(self, key: str) -> Any:
        key_path = self.keypath(key)
        data = self._read_raw(key_path)
        if data is None:
            return None
        return loads(data)

    def verify_file(self, key_path: Path) -> bool:
        read_parts = self._readparts_file(key_path)
        if read_parts is None:
            raise FileNotFoundError(f"Key {key_path} does not exist")
        data, verify_checksum = read_parts
        data_checksum = checksum(data)
        return data_checksum == verify_checksum

    def verify(self, key: str) -> bool:
        """
        Verify the integrity of a  key by comparing the stored checksum with the
        calculated checksum of the underlying data.
        Returns True of the key is valid, False if the key is invalid.
        Raises FileNotFoundError if the key does not exist.
        """
        key_path = self.keypath(key)
        if self._is_blob(key_path):
            # TODO
            raise NotImplementedError("Blob-backed key verification is not implemented yet")
        else:  # file
            return self.verify_file(key_path)

    def list(self, query: str | None = None) -> List[str]:
        if query is None:
            query = ""
        query = str(query)
        query_path = self.root.join(query)
        assert self.root in query_path.parents(), f"{self.root} is not a parent of {query_path}"
        if query_path.exists() and query_path.isdir():
            return [
                p.relative_to(self.root) for (p, t) in walk(query_path) if t in ["file", "link"]
            ]
        else:
            return []

    def delete(self, key: str) -> None:
        key = str(key)
        path = self.keypath(key)
        path.unlink(clean=self.root)

    # TODO do we need this? pipeline would be more composible.
    # TODO dont use asserts we should raise on failure.
    def iter_raw(self) -> Iterable[Tuple[str, bytes, str, bool]]:
        """Yield (key, json_bytes, stored_csum, checksum_ok) for every key."""
        for key in self.list():
            key_path = self.keypath(key)
            json_bytes = self._read_raw(key_path)
            assert json_bytes is not None
            stored_csum = self.key_blob(key)
            assert stored_csum is not None
            valid = stored_csum == checksum(json_bytes)
            yield key, json_bytes, stored_csum, valid


class KeyDBWindow:
    def __init__(self, window: str, keydb: KeyDB):
        window = str(window)
        assert isinstance(keydb, KeyDB)
        self.prefix = window + sep
        self.keydb = keydb

    def write(self, key: str, value: Any, overwrite: bool):
        assert key is not None
        assert value is not None
        # TODO this way of calculating the path is unsafe/error prone.
        self.keydb.write(self.prefix + key, value, overwrite)

    def read(self, key):
        # TODO this way of calculating the path is unsafe/error prone.
        return self.keydb.read(self.prefix + key)

    def key_blob(self, key: str) -> Optional[str]:
        return self.keydb.key_blob(self.prefix + key)

    def list(self):
        # TODO maybe relative to would be safer.
        return [x[len(self.prefix):] for x in self.keydb.list(self.prefix)]

    def delete(self, key):
        # TODO this way of calculating the path string is unsafe/error prone.
        self.keydb.delete(self.prefix + key)


class KeyDBFactory[X]:
    def __init__(
            self,
            keydb: KeyDBLike,
            encoder: Callable[[X], Any],
            decoder: Callable[[Any, str], X],
    ):
        self.keydb = keydb
        self.encoder = encoder
        self.decoder = decoder

    def write(self, key: str, value: X, overwrite: bool) -> None:
        self.keydb.write(key, self.encoder(value), overwrite)

    def read(self, key: str) -> X:
        return self.decoder(self.keydb.read(key), key)

    def key_blob(self, key: str) -> Optional[str]:
        return self.keydb.key_blob(key)

    def list(self,) -> List[str]:
        return self.keydb.list()

    def delete(self, key: str) -> None:
        self.keydb.delete(key)
