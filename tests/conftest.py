from tests.trees import generate_trees

def pytest_addoption(parser):
    parser.addoption("--all", action="store_true",
        help="run all path combinations")

def pytest_generate_tests(metafunc):
    if metafunc.config.getoption('all'):
        segments = ['a', 'b', '+']
    else:
        segments = ['a', 'b']
    csums = ['1', '2']
    if 'segments' in metafunc.fixturenames:
        metafunc.parametrize("segments", segments)
    if 'csums' in metafunc.fixturenames:
        metafunc.parametrize("csums", csums)
    if 'tree' in metafunc.fixturenames:
        metafunc.parametrize("tree", generate_trees(segments, csums))

