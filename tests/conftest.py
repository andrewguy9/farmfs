import pytest
from farmfs.fs import Path
from farmfs import getvol
from farmfs.volume import mkfs
from .trees import generate_trees
from itertools import combinations
from hashlib import md5
import io


def pytest_addoption(parser):
    parser.addoption("--all", action="store_true", help="run all path combinations")


def pytest_generate_tests(metafunc):
    if metafunc.config.getoption("all"):
        segments = ["a", "b", "+"]
    else:
        segments = ["a", "+"]
    csums = ["1", "2"]
    if "segments" in metafunc.fixturenames:
        metafunc.parametrize("segments", segments)
    if "csums" in metafunc.fixturenames:
        metafunc.parametrize("csums", csums)
    if "tree" in metafunc.fixturenames:
        metafunc.parametrize("tree", generate_trees(segments, csums))
    if "trees" in metafunc.fixturenames:
        trees = generate_trees(segments, csums)
        metafunc.parametrize("trees", combinations(trees, 2))


@pytest.fixture
def tmp(tmp_path):
    return Path(str(tmp_path))


@pytest.fixture
def vol(tmp):
    udd = tmp.join(".farmfs").join("userdata")
    mkfs(tmp, udd)
    return tmp


def build_file(root, sub_path, content, mode="w"):
    """
    Helper function to build a file under a root.
    Returns the full path of the created file.
    """
    p = Path(sub_path, root)
    with p.open(mode) as fd:
        fd.write(content)
    return p


def build_dir(root, sub_path):
    """
    Helper function to build a dir under a root.
    Returns the full path to the created dir.
    """
    p = Path(sub_path, root)
    p.mkdir()
    return p


def build_checksum(bytes):
    hash = md5()
    hash.update(bytes)
    return str(hash.hexdigest())


def build_blob(vol_path, bytes):
    def get_fake_fd():
        return io.BytesIO(bytes)

    vol = getvol(vol_path)
    csum = build_checksum(bytes)
    vol.bs.import_via_fd(get_fake_fd, csum)
    return csum


def build_link(vol_path, sub_path, blob):
    vol = getvol(vol_path)
    path = vol_path.join(sub_path)
    vol.link(path, blob)
    return path
