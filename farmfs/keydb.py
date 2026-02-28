from collections.abc import Callable
from typing import Any, Generic, Iterator, List, Optional, Protocol, Tuple, TypeVar, runtime_checkable
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


def str_diff(a: str, b: str) -> List[Tuple[int, int]]:
    """
    Return list of (start, end) half-open index ranges where a and b differ.
    Ranges are in terms of the longer string's indices.
    Adjacent or overlapping differing positions are merged into a single span.
    """
    spans: List[Tuple[int, int]] = []
    length = max(len(a), len(b))
    i = 0
    while i < length:
        if i >= len(a) or i >= len(b) or a[i] != b[i]:
            start = i
            while i < length and (i >= len(a) or i >= len(b) or a[i] != b[i]):
                i += 1
            spans.append((start, i))
        else:
            i += 1
    return spans


def diff_context(a: str, b: str, spans: List[Tuple[int, int]], ctx: int = 20) -> List[Tuple[str, str]]:
    """
    For each span in spans, extract (a_snip, b_snip) with ctx characters of
    surrounding context. Each snip is a substring of a or b respectively.
    """
    result = []
    for start, end in spans:
        a_start = max(0, start - ctx)
        a_end = min(len(a), end + ctx)
        b_start = max(0, start - ctx)
        b_end = min(len(b), end + ctx)
        result.append((a[a_start:a_end], b[b_start:b_end]))
    return result


def diff_printr(spans: List[Tuple[int, int]], context: List[Tuple[str, str]]) -> List[str]:
    """
    Format diff spans and their context snippets as human-readable lines.
    """
    lines = []
    for (start, end), (a_snip, b_snip) in zip(spans, context):
        lines.append(f"  diff at [{start}:{end}]: stored={a_snip!r} canonical={b_snip!r}")
    return lines


T = TypeVar('T')
X = TypeVar('X')


@runtime_checkable
class KeyDBLike(Protocol):
    def write(self, key: str, value: Any, overwrite: bool) -> None: ...
    def read(self, key: str) -> Any: ...   # raises FileNotFoundError if absent
    def verify(self, key: str) -> bool: ...
    def diagnose(self, key: str) -> List[str]: ...  # human-readable failure reasons; [] if ok
    def list(self, query: str | None = None) -> List[str]: ...
    def delete(self, key: str) -> None: ...


def checksum(value_bytes: bytes) -> str:
    """
    Input string should already be coersed into an encoding before being
    provided
    """
    return md5(value_bytes).hexdigest()


class BlobKeyDB:
    """Bytes-only storage layer. Reads/writes raw bytes, no JSON encoding."""

    def __init__(self, db_path: Path, tmp_dir: Path, blobstore: FileBlobstore | None = None):
        assert isinstance(db_path, Path)
        self.root = db_path
        self.tmp_dir = tmp_dir
        self.bs = blobstore

    def keypath(self, key: str) -> Path:
        key = str(key)
        return self.root.join(key)

    def _is_blob(self, key_path: Path) -> bool:
        """True if key_path is a symlink."""
        return key_path.islink()

    def _readparts_file(self, key_path: Path) -> Tuple[bytes, str]:
        """
        Read both the value bytes and checksum from a file-backed key.
        Raises FileNotFoundError if the key does not exist.
        """
        try:
            with key_path.open("rb") as f:
                obj_bytes = f.readline().strip()
                verify_checksum = f.readline().strip()
                return obj_bytes, verify_checksum.decode("utf-8")
        except IOError as e:
            if e.errno == NoSuchFile or e.errno == IsDirectory:
                raise FileNotFoundError(f"Key {key_path} does not exist") from e
            else:
                raise e

    def read(self, key: str) -> bytes:
        """
        Read the raw bytes for a key.
        Raises FileNotFoundError if the key is absent or the symlink is dangling.
        """
        key_path = self.keypath(key)
        if self._is_blob(key_path):
            # Dangling symlink: open() raises FileNotFoundError — propagate.
            with key_path.open("rb") as f:
                return f.read()
        else:
            data, _ = self._readparts_file(key_path)
            return data

    def write(self, key: str, value: bytes, overwrite: bool) -> None:
        """
        Write raw bytes as a blob-backed key.
        Raises ValueError if key exists and overwrite=False.
        Raises RuntimeError if no blobstore is configured.
        """
        key = str(key)
        key_path = self.keypath(key)
        if key_path.exists() and not overwrite:
            raise ValueError("Key %s already exists" % key)
        if self.bs is None:
            raise RuntimeError("No blobstore — read-only bootstrap mode")
        value_hash = checksum(value)
        self.bs.import_via_fd(lambda: BytesIO(value), value_hash)
        blob_path = self.bs.blob_path(value_hash)
        ensure_symlink(key_path, blob_path)

    def verify(self, key: str) -> bool:
        """
        Verify integrity of a key.
        Returns True if valid, False if corrupted.
        Raises FileNotFoundError if key is absent or symlink is dangling.
        Raises RuntimeError if no blobstore is configured (blob-backed key).
        """
        key_path = self.keypath(key)
        if self._is_blob(key_path):
            if self.bs is None:
                raise RuntimeError("No blobstore — read-only bootstrap mode")
            csum = self._key_blob(key)
            computed = self.bs.blob_checksum(csum)
            return computed == csum
        else:
            data, stored_csum = self._readparts_file(key_path)
            return checksum(data) == stored_csum

    def _key_blob(self, key: str) -> str:
        """
        Return the blob checksum referenced by this key's symlink.
        Raises FileNotFoundError if key is absent or not a symlink.
        """
        key_path = self.keypath(key)
        if not self._is_blob(key_path):
            raise FileNotFoundError(f"Key {key} is not blob-backed")
        if self.bs is None:
            raise RuntimeError("No blobstore — read-only bootstrap mode")
        link_path = key_path.readlink()
        return self.bs.reverser(link_path)

    def diagnose(self, key: str) -> List[str]:
        """Storage-level diagnose: empty because BlobKeyDB has no semantic/JSON checks."""
        return []

    def live_blobs(self) -> Iterator[str]:
        """Yield csums of all blobs referenced by blob-backed keys."""
        for key in self.list():
            key_path = self.keypath(key)
            if self._is_blob(key_path):
                yield self._key_blob(key)

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


# Backwards-compat alias — callers that import KeyDB still work.
KeyDB = BlobKeyDB


class JsonKeyDB:
    """JSON serialisation layer wrapping BlobKeyDB."""

    def __init__(self, db: BlobKeyDB):
        self.db = db

    def read(self, key: str) -> Any:
        """
        Read and JSON-decode a key.
        Raises FileNotFoundError if key is absent.
        """
        raw = self.db.read(key)
        return loads(raw)

    def write(self, key: str, value: Any, overwrite: bool) -> None:
        value_str = keydb_encoder.encode(value)
        value_bytes = egest(value_str)
        self.db.write(key, value_bytes, overwrite)

    def verify(self, key: str) -> bool:
        """
        Verify round-trip invariant: encode(decode(raw)) == raw.
        Raises FileNotFoundError if key is absent.
        """
        raw = self.db.read(key)
        decoded = loads(raw)
        re_encoded = egest(keydb_encoder.encode(decoded))
        return re_encoded == raw

    def diagnose(self, key: str) -> List[str]:
        """
        Return human-readable reasons why verify() failed.
        Returns [] if the key is valid.
        Raises FileNotFoundError if key is absent.
        """
        raw = self.db.read(key)
        decoded = loads(raw)
        re_encoded = egest(keydb_encoder.encode(decoded))
        if re_encoded == raw:
            return []
        # The parsed value is always identical to decoded; the difference is
        # purely in how the JSON was serialised. Decode to str and diff.
        stored_str = raw.decode("utf-8")
        canon_str = re_encoded.decode("utf-8")
        spans = str_diff(stored_str, canon_str)
        context = diff_context(stored_str, canon_str, spans)
        header = f"stored {len(stored_str)} chars, canonical {len(canon_str)} chars (data intact, needs rewrite)"
        return [header] + diff_printr(spans, context)

    def list(self, query: str | None = None) -> List[str]:
        return self.db.list(query)

    def delete(self, key: str) -> None:
        self.db.delete(key)


class KeyDBWindow:
    """Namespace prefix over any KeyDB-like layer."""

    def __init__(self, window: str, keydb: KeyDBLike):
        window = str(window)
        self.prefix = window + sep
        self.keydb = keydb

    def write(self, key: str, value: Any, overwrite: bool) -> None:
        assert key is not None
        assert value is not None
        self.keydb.write(self.prefix + key, value, overwrite)

    def read(self, key: str) -> Any:
        return self.keydb.read(self.prefix + key)

    def verify(self, key: str) -> bool:
        return self.keydb.verify(self.prefix + key)

    def diagnose(self, key: str) -> List[str]:
        return self.keydb.diagnose(self.prefix + key)

    def list(self, query: str | None = None) -> List[str]:
        effective_query = self.prefix if query is None else self.prefix + query
        return [x[len(self.prefix):] for x in self.keydb.list(effective_query)]

    def delete(self, key: str) -> None:
        self.keydb.delete(self.prefix + key)


class KeyDBFactory(Generic[X]):
    def __init__(
            self,
            keydb: KeyDBLike,
            encoder: Callable[[X], Any],
            decoder: Callable[[Any, str], X],
            validate: Optional[Callable[[str, X], List[str]]] = None,
    ):
        self.keydb = keydb
        self.encoder = encoder
        self.decoder = decoder
        self.validate = validate

    def write(self, key: str, value: X, overwrite: bool) -> None:
        self.keydb.write(key, self.encoder(value), overwrite)

    def read(self, key: str) -> X:
        """Raises FileNotFoundError if absent."""
        return self.decoder(self.keydb.read(key), key)

    def verify(self, key: str) -> bool:
        """
        Domain-level validation via validate callback (if set).
        Raises FileNotFoundError if key is absent.
        """
        return len(self.diagnose(key)) == 0

    def diagnose(self, key: str) -> List[str]:
        """
        Return human-readable reasons why verify() failed.
        Returns [] if the key is valid.
        Raises FileNotFoundError if key is absent.
        """
        value = self.read(key)
        if self.validate is None:
            return []
        return self.validate(key, value)

    def list(self, query: str | None = None) -> List[str]:
        return self.keydb.list(query)

    def delete(self, key: str) -> None:
        self.keydb.delete(key)
