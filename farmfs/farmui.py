import argparse
import farmfs
from farmfs.fs import Path

parser = argparse.ArgumentParser(description='Manage a farmfs instance.',)

verb_parsers = parser.add_subparsers()

mkfs_parser = verb_parsers.add_parser('mkfs')
mkfs_parser.set_defaults(verb=farmfs.mkfs)
mkfs_parser.add_argument('--root', default=".", type=Path)

findvol_parser = verb_parsers.add_parser("findvol")
findvol_parser.set_defaults(verb=farmfs.findvol)

writekey_parser = verb_parsers.add_parser("writekey")
writekey_parser.set_defaults(verb=farmfs.writekey)
writekey_parser.add_argument("key", type=str)
writekey_parser.add_argument("value")

readkey_parser = verb_parsers.add_parser("readkey")
readkey_parser.set_defaults(verb=farmfs.readkey)
readkey_parser.add_argument("key")

list_keys_parser = verb_parsers.add_parser("listkeys")
list_keys_parser.set_defaults(verb=farmfs.list_keys)

status_parser = verb_parsers.add_parser("status")
status_parser.set_defaults(verb=farmfs.status)
status_parser.add_argument("paths", nargs='*', type=Path, default=[Path(".")])

freeze_parser = verb_parsers.add_parser("freeze")
freeze_parser.set_defaults(verb=farmfs.freeze)
freeze_parser.add_argument("files", nargs='*', type=Path, default=[Path('.')])

thaw_parser = verb_parsers.add_parser("thaw")
thaw_parser.set_defaults(verb=farmfs.thaw)
thaw_parser.add_argument("files", nargs='*', type=Path, default=[Path('.')])

fsck_parser = verb_parsers.add_parser("fsck")
fsck_parser.set_defaults(verb=farmfs.fsck)

walk_parser = verb_parsers.add_parser("walk")
walk_parser.set_defaults(verb=farmfs.walk)
walk_group = walk_parser.add_mutually_exclusive_group(required=True)
walk_group.add_argument("--keys", dest="walk", action="store_const", const="keys")
walk_group.add_argument("--userdata", dest="walk", action="store_const", const="userdata")
walk_group.add_argument("--root", dest="walk", action="store_const", const="root")

count_parser = verb_parsers.add_parser("count")
count_parser.set_defaults(verb=farmfs.count)

dup_parser = verb_parsers.add_parser("dup")
dup_parser.set_defaults(verb=farmfs.dup)

reverse_parser = verb_parsers.add_parser("reverse")
reverse_parser.set_defaults(verb=farmfs.reverse)
reverse_parser.add_argument("udd_name", type=Path)

gc_parser = verb_parsers.add_parser("gc")
gc_parser.set_defaults(verb=farmfs.gc)

snap_parser = verb_parsers.add_parser("snap")
snap_parser.set_defaults(verb=farmfs.snap)
snap_parser.add_argument("action", choices=['make', 'list', 'read', 'delete', 'restore'])
snap_parser.add_argument("name", nargs='?')

csum_parser = verb_parsers.add_parser("checksum")
csum_parser.set_defaults(verb=farmfs.csum)
csum_parser.add_argument("name", nargs='+')

"""Builds a symlink farm out of a directory.
Looks at a the directory and turns all the
files into symlinks into the md5 sums of the files
under .farmfs/data"""

def main():
  args = parser.parse_args();
  args.verb(args)

if __name__ == "__main__":
  main()
