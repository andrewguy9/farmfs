from __future__ import print_function
from docopt import docopt
from farmfs import getvol
from farmfs import reverse
from farmfs import cwd
from farmfs.util import empty2dot
from farmfs.volume import encode_snapshot
from func_prototypes import constructors
from farmfs.fs import Path
from json import loads, JSONEncoder
from functools import partial
import sys
try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map

def printNotNone(value):
  if value is not None:
    print(value)

def walk(parents, exclude, match):
  for parent in parents:
    for (path, type_) in parent.entries(exclude):
      if type_ in match:
        yield (path, type_)

USAGE = \
"""
FarmDBG

Usage:
  farmdbg findvol
  farmdbg reverse <link>
  farmdbg key read <key>
  farmdbg key write <key> <value>
  farmdbg key delete <key>
  farmdbg key list [<key>]
  farmdbg walk (keys|userdata|root|snap <snapshot>)
  farmdbg checksum <path>...
  farmdbg fix link <file> <target>
  farmdbg rewrite-links <target>
"""

def main():
  args = docopt(USAGE)
  vol = getvol(cwd)
  if args['findvol']:
    print("Volume found at: %s" % vol.root)
  elif args['reverse']:
    path = Path(args['<link>'], cwd)
    for p in reverse(vol, path):
      print(p)
  elif args['key']:
    db = vol.keydb
    key = args['<key>']
    if args['read']:
      printNotNone(db.readraw(key))
    elif args['delete']:
      db.delete(key)
    elif args['list']:
      for v in db.list(key):
        print(v)
    elif args['write']:
      value = args['<value>']
      db.write(key, value)
  elif args['walk']:
    if args['root']:
      print(JSONEncoder(ensure_ascii=False).encode(encode_snapshot(vol.tree())))
    elif args['snap']:
      print(JSONEncoder(ensure_ascii=False).encode(encode_snapshot(vol.snapdb.read(args['<snapshot>']))))
    elif args['userdata']:
      print(JSONEncoder(ensure_ascii=False).encode(list(map(safetype, map(lambda x: x[0], walk([vol.udd], [safetype(vol.mdd)], ["file"]))))))
    elif args['keys']:
      print(JSONEncoder(ensure_ascii=False).encode(vol.keydb.list()))
  elif args['checksum']:
    #TODO <checksum> <full path>
    paths = imap(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    for p in paths:
      print(p.checksum(), p.relative_to(cwd, leading_sep=False))
  elif args['link']:
    f = Path(args['<file>'], cwd)
    t = Path(args['<target>'], cwd)
    if not f.islink():
      raise ValueError("%s is not a link. Refusing to fix" % (f))
    f.unlink()
    f.symlink(t)
  elif args['rewrite-links']:
    target = Path(args['<target>'], cwd)
    for (link, _type) in walk([target], [safetype(vol.mdd)], ["link"]):
      new = vol.repair_link(link)
      if new is not None:
          print("Relinked %s to %s" % (link.relative_to(cwd, leading_sep=False), new))
