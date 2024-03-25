import pytest
from farmfs.blobstore import _old_reverser as old_reverser
from farmfs.blobstore import _fast_reverser as fast_reverser

@pytest.mark.parametrize(
    "reverser_builder", [old_reverser, fast_reverser])
def test_reverser(reverser_builder):
    input = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    output = "d41d8cd98f00b204e9800998ecf8427e"
    assert reverser_builder(3)(input) == output
