farmfs
======

Tool for creating / distributing / maintaining symlink farms.

## Usage:
```
farmfs mkfs [--root ROOT]\n
farmfs findvol [-h]
farmfs key {read,write,delete,list} [name] [value]
farmfs status [paths [paths ...]]
farmfs freeze [files [files ...]]
farmfs thaw [files [files ...]]
farmfs fsck [-h]
farmfs walk (--keys | --userdata | --root)
farmfs count [-h]
farmfs dup [-h]
farmfs reverse udd_name
farmfs gc [-h]
farmfs reverse udd_name
farmfs snap {make,list,read,delete,restore} [name]
farmfs checksum name [name ...]
farmfs remote {add,remove,list} [name] [location]
farmfs pull remote [snap]
```
## What is FarmFS

Farmfs is a git style interface to non text, usually immutable, sometimes large files.
It takes your files and puts them into an immutable blob store then builds symlinks from the file names into the store.

### Why would you do that?
* You can snapshot your directory structure BIG_O(num_files).
* You can diff two different farmfs stores with BIG_O(num_files) rather than BIG_O(sum(file_sizes))
* You can identify corruption of your files because all entries in the blob store are checksumed.
* If the same file contents appear in multiple places you only have to put it in the blob store once. (deduplication)

## Getting Started

Create a Farmfs store

`mkdir myfarm
cd myfarm
farmfs mkfs`

Make some files

```
mkdir -p 1/2/3/4/5
mkdir -p a/b/c/d/e
echo "value1" > 1/2/3/4/5/v1
echo "value1" > a/b/c/d/e/v1
```

See the uncommitted files

```
farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1
/Users/andrewguy9/Downloads/readme/a/b/c/d/e/v1
```

Add the untracked files to the blob store.
Notice it only needs to store "value1" once.

```
farmfs freeze
Processing /Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55
Processing /Users/andrewguy9/Downloads/readme/a/b/c/d/e/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Found a copy of file already in userdata, skipping copy
```

Edit a file
First we need to thaw it.

```
farmfs thaw 1/2/3/4/5/v1
farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1
echo "value2" > 1/2/3/4/5/v1
farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1
  farmfs freeze 1/2/3/4/5/v1
Processing /Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a
farmfs status
```

