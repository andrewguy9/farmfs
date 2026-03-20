import io
import threading
import uuid

import pytest
from werkzeug.serving import make_server

from farmfs.api import get_app
from farmfs.blobstore import FileBlobstore, HttpBlobstore, LifecycleError, S3Blobstore, fast_reverser, old_reverser
from farmfs.fs import is_readonly
from farmfs.volume import mkfs
from .conftest import build_checksum


@pytest.mark.parametrize("reverser_builder", [old_reverser, fast_reverser])
def test_reverser(reverser_builder):
    input = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    output = "d41d8cd98f00b204e9800998ecf8427e"
    assert reverser_builder(3)(input) == output


def test_S3Blobstore_url():
    s3url = "s3://testbucket/testprefix"
    access_id = "test_access_id"
    secret = b"test_secret_id"
    s3 = S3Blobstore(s3url, access_id, secret)
    blob = "60b725f10c9c85c70d97880dfe8191b3"
    url = s3.url(blob)
    # TODO new_expected = "https://%s/%s/%s" % (bucket, prefix, blob)
    old_expected = "https://s3.amazonaws.com/%s/%s/%s" % (
        "testbucket",
        "testprefix",
        blob,
    )
    assert url == old_expected
    # TODO Add support for new style urls too.


def test_file_import_via_fd(tmp):
    ud = tmp.join("userdata")
    ud.mkdir()
    scratch = tmp.join("tmp")
    scratch.mkdir()
    bs = FileBlobstore(ud, scratch)
    payload = b"foo"
    blob = build_checksum(payload)
    src_fn = lambda: io.BytesIO(payload)
    dst = bs.blob_path(blob)
    assert not dst.exists()
    with bs.session() as sess:
        sess.import_via_fd(src_fn, blob)
    assert dst.isfile()
    assert dst.checksum() == blob
    assert is_readonly(dst)
    assert bs.verify_blob_permissions(blob)


# ---------------------------------------------------------------------------
# Lifecycle tests — parametrized across FileBlobstore and HttpBlobstore
# ---------------------------------------------------------------------------

_LIFECYCLE_PAYLOAD = b"lifecycle-payload"
_BS_PORT = 5009


class _MockServerThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__(daemon=True)
        self.server = make_server("127.0.0.1", port, app)
        app.app_context().push()

    def run(self):
        self.server.serve_forever()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.join()


@pytest.fixture(params=["file", "api", "s3"])
def bs_with_blob(request, tmp):
    """Yields (blobstore, blob) pre-populated with _LIFECYCLE_PAYLOAD."""
    blob = build_checksum(_LIFECYCLE_PAYLOAD)
    if request.param == "file":
        ud = tmp.join("userdata")
        ud.mkdir()
        scratch = tmp.join("tmp")
        scratch.mkdir()
        bs = FileBlobstore(ud, scratch)
        with bs.session() as sess:
            sess.import_via_fd(lambda: io.BytesIO(_LIFECYCLE_PAYLOAD), blob)
        yield bs, blob
    elif request.param == "api":
        server_root = tmp.join("api_server")
        server_root.mkdir()
        udd = server_root.join(".farmfs").join("userdata")
        mkfs(server_root, udd)
        app = get_app({"<root>": str(server_root)})
        with _MockServerThread(app, _BS_PORT):
            bs = HttpBlobstore(f"http://127.0.0.1:{_BS_PORT}", conn_timeout=5)
            with bs.session() as sess:
                sess.import_via_fd(lambda: io.BytesIO(_LIFECYCLE_PAYLOAD), blob)
            yield bs, blob
    elif request.param == "s3":
        try:
            from s3lib.ui import load_creds as load_s3_creds
            access_id, secret = load_s3_creds(None)
        except Exception:
            pytest.skip("S3 credentials not available")
        s3_url = "s3://s3libtestbucket/" + str(uuid.uuid4())
        bs = S3Blobstore(s3_url, access_id, secret)
        with bs.session() as sess:
            sess.import_via_fd(lambda: io.BytesIO(_LIFECYCLE_PAYLOAD), blob)
        yield bs, blob


def test_session_nested_read_handles(bs_with_blob):
    """Opening a second read_handle while one is still open raises LifecycleError."""
    bs, blob = bs_with_blob
    with bs.session() as sess:
        with sess.read_handle(blob):
            with pytest.raises(LifecycleError):
                with sess.read_handle(blob):
                    pass


def test_session_exit_with_open_handle(bs_with_blob):
    """Exiting the session while a read handle is still open raises LifecycleError."""
    bs, blob = bs_with_blob
    sess = bs.session()
    sess.__enter__()
    handle_ctx = sess.read_handle(blob)
    handle_ctx.__enter__()
    with pytest.raises(LifecycleError):
        sess.__exit__(None, None, None)
    handle_ctx.__exit__(None, None, None)


def test_session_enter_with_open_handle(bs_with_blob):
    """Re-entering a session that already has an open handle raises LifecycleError."""
    bs, blob = bs_with_blob
    sess = bs.session()
    sess.__enter__()
    handle_ctx = sess.read_handle(blob)
    handle_ctx.__enter__()
    with pytest.raises(LifecycleError):
        sess.__enter__()
    handle_ctx.__exit__(None, None, None)
