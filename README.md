farmfs
======

Tool for creating / distributing / maintaining symlink farms.

## Warning
FarmFS is still very early stage software. 

Please do not keep anything in it which you are not willing to lose.

## Installation

### To use Farmfs

pip install git+https://github.com/andrewguy9/farmfs.git@master

### To hack on Farmfs
```
git clone https://github.com/andrewguy9/farmfs.git
cd farmfs
python setup.py install
```

## Usage:
```
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

```
mkdir myfarm
cd myfarm
farmfs mkfs
```

Make some files

```
mkdir -p 1/2/3/4/5
mkdir -p a/b/c/d/e
echo "value1" > 1/2/3/4/5/v1
echo "value1" > a/b/c/d/e/v1
```

Status can show us unmanged files.

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

Edit a file.
First we need to thaw it, then we can change it.

```
farmfs thaw 1/2/3/4/5/v1

farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1

echo "value2" > 1/2/3/4/5/v1

farmfs freeze 1/2/3/4/5/v1
Processing /Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a

farmfs status
```

We don't want to loose our progress, so lets make a snapshot.

```
farmfs snap make mysnap
```

Now create more stuff

```
echo "oops" > mistake.txt

farmfs freeze mistake.txt
Processing /Users/andrewguy9/Downloads/readme/mistake.txt with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/38a/f5c/549/26b620264ab1501150cf189
```

Well that was a mistake, lets roll back to the old snap.

```
farmfs snap restore mysnap
Removing /mistake.txt
```

Now that we have our files built, lets build another depot.

```
cd ..
mkdir copy
cd copy
farmfs mkfs
```

We want to add our prior depot as a remote.

```
farmfs remote add origin ../myfarm
```

Now lets copy our work from before.

```
farmfs pull origin
mkdir /1
mkdir /1/2
mkdir /1/2/3
mkdir /1/2/3/4
mkdir /1/2/3/4/5
mklink /1/2/3/4/5/v1 -> /4ca/8c5/ae5/e759e237bfb80c51940de7a
Blob missing from local, copying
*** /Users/andrewguy9/Downloads/copy/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a /Users/andrewguy9/Downloads/myfarm/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a
mkdir /a
mkdir /a/b
mkdir /a/b/c
mkdir /a/b/c/d
mkdir /a/b/c/d/e
mklink /a/b/c/d/e/v1 -> /238/851/a91/77b60af767ca431ed521e55
Blob missing from local, copying
*** /Users/andrewguy9/Downloads/copy/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55 /Users/andrewguy9/Downloads/myfarm/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55
```

Lets see whats in our new depot:

```
find *
1
1/2
1/2/3
1/2/3/4
1/2/3/4/5
1/2/3/4/5/v1
a
a/b
a/b/c
a/b/c/d
a/b/c/d/e
a/b/c/d/e/v1
```
