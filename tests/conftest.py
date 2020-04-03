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

