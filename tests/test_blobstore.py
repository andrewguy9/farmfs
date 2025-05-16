import pytest
from farmfs.blobstore import old_reverser, fast_reverser, FileBlobstore, S3Blobstore
from .conftest import build_checksum
from farmfs.fs import is_readonly
import io

@pytest.mark.parametrize(
    "reverser_builder", [old_reverser, fast_reverser])
def test_reverser(reverser_builder):
    input = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    output = "d41d8cd98f00b204e9800998ecf8427e"
    assert reverser_builder(3)(input) == output

def test_S3Blobstore_url():
    s3url = 's3://testbucket/testprefix'
    access_id = "test_access_id"
    secret = b"test_secret_id"
    s3 = S3Blobstore(s3url, access_id, secret)
    blob = "60b725f10c9c85c70d97880dfe8191b3"
    url = s3.url(blob)
    # TODO new_expected = "https://%s/%s/%s" % (bucket, prefix, blob)
    old_expected = "https://s3.amazonaws.com/%s/%s/%s" % ('testbucket', 'testprefix', blob)
    assert url == old_expected
    # TODO Add support for new style urls too.

def test_file_import_via_fd(tmp):
    ud = tmp.join("userdata")
    ud.mkdir()
    scratch = tmp.join("tmp")
    scratch.mkdir()
    bs = FileBlobstore(ud, scratch)
    payload=b'foo'
    blob = build_checksum(payload)
    src_fn = lambda: io.BytesIO(payload)
    dst = bs.blob_path(blob)
    assert not dst.exists()
    bs.import_via_fd(src_fn, blob)
    assert dst.isfile()
    assert dst.checksum() == blob
    assert is_readonly(dst)
    assert bs.verify_blob_permissions(blob)
