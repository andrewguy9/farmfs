import pytest
from farmfs.blobstore import _reverser as reverser

@pytest.mark.parametrize(
    "reverser_builder", [reverser])
def test_reverser(reverser_builder):
    input = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    output = "d41d8cd98f00b204e9800998ecf8427e"
    assert reverser_builder(3)(input) == output
