from collections.abc import Callable
from typing import Any, List
from farmfs.fs import Path
from farmfs.fs import ensure_file
from farmfs.fs import walk
from hashlib import md5
from json import loads, JSONEncoder
from errno import ENOENT as NoSuchFile
from errno import EISDIR as IsDirectory
from os.path import sep
from farmfs.util import egest

keydb_encoder = JSONEncoder(ensure_ascii=False, sort_keys=True)


def checksum(value_bytes: bytes) -> str:
    """
    Input string should already be coersed into an encoding before being
    provided
    """
    return md5(value_bytes).hexdigest()


class KeyDB:
    def __init__(self, db_path: Path):
        assert isinstance(db_path, Path)
        self.root = db_path

    # TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME
    # ITERATOR BASED RECORD TYPE.
    def write(self, key: str, value: Any, overwrite: bool) -> None:
        key = str(key)
        key_path = self.root.join(key)
        if key_path.exists() and not overwrite:
            raise ValueError("Key %s already exists" % key)
        value_str = keydb_encoder.encode(value)
        value_bytes = egest(value_str)
        value_hash = egest(checksum(value_bytes))
        with ensure_file(key_path, "wb") as f:
            f.write(value_bytes)
            f.write(b"\n")
            f.write(value_hash)
            f.write(b"\n")

    def readraw(self, key: str) -> bytes | None:
        key = str(key)
        try:
            with self.root.join(key).open("rb") as f:
                obj_bytes = f.readline().strip()
                obj_bytes_checksum = checksum(obj_bytes).encode("utf-8")
                key_checksum = f.readline().strip()
            if obj_bytes_checksum != key_checksum:
                raise ValueError(
                    "Checksum mismatch for key %s. Expected %s, calculated %s"
                    % (key, key_checksum, obj_bytes_checksum)
                )
            obj_bytes = egest(obj_bytes)
            return obj_bytes
        except IOError as e:
            if e.errno == NoSuchFile or e.errno == IsDirectory:
                return None
            else:
                raise e

    def read(self, key: str) -> Any:
        obj_bytes = self.readraw(key)
        if obj_bytes is None:
            return None
        else:
            obj = loads(obj_bytes)
            return obj

    def list(self, query: str | None = None) -> List[str]:
        if query is None:
            query = ""
        query = str(query)
        query_path = self.root.join(query)
        assert self.root in query_path.parents(), "%s is not a parent of %s" % (
            self.root,
            query_path,
        )
        if query_path.exists() and query_path.isdir():
            return [
                p.relative_to(self.root) for (p, t) in walk(query_path) if t == "file"
            ]
        else:
            return []

    def delete(self, key: str) -> None:
        key = str(key)
        path = self.root.join(key)
        path.unlink(clean=self.root)


class KeyDBWindow(KeyDB):
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

    def list(self):
        # TODO maybe relative to would be safer.
        return [x[len(self.prefix) :] for x in self.keydb.list(self.prefix)]

    def delete(self, key):
        # TODO this way of calculating the path string is unsafe/error prone.
        self.keydb.delete(self.prefix + key)


class KeyDBFactory[X]:
    def __init__(
            self,
            keydb: KeyDB,
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

    def list(self,) -> List[str]:
        return self.keydb.list()

    def delete(self, key: str) -> None:
        self.keydb.delete(key)
