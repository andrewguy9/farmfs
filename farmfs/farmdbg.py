from docopt import docopt
import farmfs

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
  if args['findvol']:
    farmfs.findvol(".")
  elif args['reverse']:
    farmfs.reverse(args['<link>'])
  elif args['key']:
    if args['read']:
      farmfs.key('read', args['<key>'])
    elif args['write']:
      farmfs.key('write', args['<key>'], args['<value>'])
    elif args['delete']:
      farmfs.key('delete', args['<key>'])
    elif args['list']:
      farmfs.key('list', args['<key>'])
  elif args['walk']:
    if args['root']:
      farmfs.walk('root')
    elif args['userdata']:
      farmfs.walk('userdata')
    elif args['keys']:
      farmfs.walk('keys')

