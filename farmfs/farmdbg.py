from docopt import docopt
from farmfs import getvol
from farmfs import makePath
from farmfs import reverse
from farmfs.util import empty2dot
from func_prototypes import constructors

def printNotNone(value):
  if value is not None:
    print value

def walk(parents, exclude, match):
  for parent in parents:
    for (path, type_) in parent.entries(exclude):
      if type_ in match:
        if type_ == "link":
          print type_, path, path.readlink()
        else:
          print type_, path

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
"""

def main():
  args = docopt(USAGE)
  vol = getvol(makePath("."))
  if args['findvol']:
    print "Volume found at: %s" % vol.root
  elif args['reverse']:
    path = makePath(args['<link>'])
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
      walk([vol.root], [vol.mdd], ["file", "dir", "link"])
    elif args['userdata']:
      walk([vol.udd], [vol.mdd], ["file"])
    elif args['keys']:
      walk([vol.keydbd], [vol.mdd], ["file"])
  elif args['checksum']:
    paths = map(makePath, empty2dot(args['<path>']))
    for p in paths:
      print p.checksum(), p
  elif args['link']:
    f = makePath(args['<file>'])
    t = makePath(args['<target>'])
    if not f.islink():
      raise ValueError("%s is not a link. Refusing to fix" % (f))
    f.unlink()
    f.symlink(t)


