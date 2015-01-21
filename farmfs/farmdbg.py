from docopt import docopt
from farmfs import getvol

def printNotNone(value):
  if value is not None:
    print value

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
"""

def main():
  args = docopt(USAGE)
  vol = getvol(Path("."))
  if args['findvol']:
    print "Volume found at: %s" % vol.root()
  elif args['reverse']:
    farmfs.reverse(vol, args['<link>'])
  elif args['key']:
    db = vol.keydb
    key = args['<key>']
    if args['read']:
      printNotNone(db.read(key))
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
      farmfs.walk('root')
    elif args['userdata']:
      farmfs.walk('userdata')
    elif args['keys']:
      farmfs.walk('keys')

