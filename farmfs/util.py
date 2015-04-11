""""
If zero length array is passed, returns ["."].
Otherwise returns the origional array.
Useful for dealing with lists of files, or not.
"""
def empty2dot(paths):
  if len(paths) == 0:
    return ["."]
  else:
    return paths

def test_empty2dot():
  assert empty2dot([]) == ["."]
  l = [1,2,3]
  assert empty2dot(l) == l

