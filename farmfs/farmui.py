import farmfs
from farmfs import getvol
from farmfs.fs import Path #TODO REMOVE
from docopt import docopt

USAGE = \
"""
FarmFS

Usage:
  farmfs mkfs
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap (make|list|read|delete|restore) <snap>
  farmfs fsck
  farmfs count
  farmfs similarity
  farmfs gc
  farmfs checksum <path>...
  farmfs remote add <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list
  farmfs pull <remote> [<snap>]


Options:

"""

def empty2dot(paths):
  if len(paths) == 0:
    return ["."]
  else:
    return paths

def str2paths(paths): #TODO MAYBE RENAME TO MAKEPATHS, might work with windows someday.
  return map(Path, paths) #TODO maybe move to farmfs...

def status(vol, paths):
  paths = map(Path, paths) #TODO ACTUALLY USE str2paths
  for thawed in vol.thawed(paths):
    print thawed

def main():
  args = docopt(USAGE)
  exitcode = 0
  if args['mkfs']:
    farmfs.mkfs('.') #TODO HOW IS THIS NOT A PATH?!
  else:
    vol = getvol(Path('.')) #TODO GET VOL SHOULD TAKE A STRING?
    paths = str2paths(empty2dot(args['<path>']))
    if args['status']:
      status(vol, paths)
    elif args['freeze']:
      vol.freeze(paths)
    elif args['thaw']:
      vol.thaw(paths)
    elif args['fsck']:
      for corruption in vol.fsck():
        exitcode = 1
        print corruption
    elif args['count']:
      for f, c in vol.count().items():
        print c, f
    elif args['similarity']:
      for (dir_a, dir_b, sim) in vol.similarity():
        print sim, dir_a, dir_b
    elif args['gc']:
      for f in farmfs.gc(vol):
        print "Removing", f
    elif args['snap']:
      snap_verbs = "make list read delete restore".split(" ")
      verb = snap_verbs[map(args.get, snap_verbs).index(True)]
      farmfs.snap(verb, args['<snap>'])
    elif args['checksum']:
      for p in args['<path>']:
        print farmfs.checksum(p), p #TODO HOW DOES CHECKSUM TAKE A STRING?!
    elif args['remote']:
      remote_verbs = "add remove list".split(" ")
      if args["add"]:
        farmfs.remote_add(vol, args['<remote>'], args['<root>'])
      elif args["remove"]:
        farmfs.remote_remove(vol, args['<remote>'])
      elif args["list"]:
        farmfs.remote_list(vol)
    elif args['pull']:
      farmfs.pull(args['<remote>'], args['<snap>'])
  exit(exitcode)
