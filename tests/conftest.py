import pytest
from farmfs.fs import Path
from farmfs.volume import mkfs
from .trees import generate_trees
from itertools import combinations

def pytest_addoption(parser):
    parser.addoption("--all", action="store_true",
                     help="run all path combinations")

def pytest_generate_tests(metafunc):
    if metafunc.config.getoption('all'):
        segments = ['a', 'b', '+']
    else:
        segments = ['a', '+']
    csums = ['1', '2']
    if 'segments' in metafunc.fixturenames:
        metafunc.parametrize("segments", segments)
    if 'csums' in metafunc.fixturenames:
        metafunc.parametrize("csums", csums)
    if 'tree' in metafunc.fixturenames:
        metafunc.parametrize("tree", generate_trees(segments, csums))
    if 'trees' in metafunc.fixturenames:
        trees = generate_trees(segments, csums)
        metafunc.parametrize("trees", combinations(trees, 2))

@pytest.fixture
def tmp(tmp_path):
    return Path(str(tmp_path))

@pytest.fixture
def vol(tmp):
    udd = tmp.join('.farmfs').join('userdata')
    mkfs(tmp, udd)
    return tmp
