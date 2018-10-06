from docopt import docopt
from farmfs import getvol
from farmfs import reverse
from farmfs.util import empty2dot
from farmfs.snapshot import encode_snapshot
from func_prototypes import constructors
from os import getcwdu
from fs import Path
from json import loads, JSONEncoder
from functools import partial

def printNotNone(value):
  if value is not None:
    print value

def print_file((path, type_)):
  if type_ == "link":
    print type_, path, path.readlink()
  else:
    print type_, path

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
  farmdbg walk (keys|userdata|root)
  farmdbg checksum <path>...
  farmdbg fix link <file> <target>
  farmdbg rewrite-links <target>
"""

def main():
  args = docopt(USAGE)
  cwd = Path(getcwdu())
  vol = getvol(cwd)
  if args['findvol']:
    print "Volume found at: %s" % vol.root
  elif args['reverse']:
    path = Path(args['<link>'], cwd)
    for p in reverse(vol, path):
      print p
  elif args['key']:
    db = vol.keydb
    key = args['<key>']
    if args['read']:
      printNotNone(db.readraw(key))
    elif args['delete']:
      db.delete(key)
    elif args['list']:
      for v in db.list(key):
        print v
    elif args['write']:
      value = args['<value>']
      db.write(key, value)
  elif args['walk']:
    if args['root']:
      print JSONEncoder().encode(encode_snapshot(vol.tree()))
    elif args['userdata']:
      map(print_file, walk([vol.udd], [str(vol.mdd)], ["file"]))
    elif args['keys']:
      print JSONEncoder().encode(vol.keydb.list())
  elif args['checksum']:
    paths = map(lambda x: Path(x, cwd), empty2dot(args['<path>']))
    for p in paths:
      print p.checksum(), p
  elif args['link']:
    f = Path(args['<file>'], cwd)
    t = Path(args['<target>'], cwd)
    if not f.islink():
      raise ValueError("%s is not a link. Refusing to fix" % (f))
    f.unlink()
    f.symlink(t)
  elif args['rewrite-links']:
    target = Path(args['<target>'], cwd)
    for (link, _type) in walk([target], [str(vol.mdd)], ["link"]):
      vol.repair_link(link)
