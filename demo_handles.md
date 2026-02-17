# Managed IO Pipelines:

How to get automatic retires, resource reuse and paralelism without leaking
connections or complicating your code.

## Strategy:

Don't try to manage connections, sockets, and file handles directly.
Intead, use thunks which *create* the resources when needed. 
Then you can use a side-effect managment function which can take responsibility
for acquiring and releasing these limited resources for you.

IO bracketing functions separate the description of a resource from its acquisition, 
and can be composed a-la-cart giving you benefits like automatic retries,
connection pooling/reuse, and parallelism with much less effort and fewer resource leak bugs.

## The building blocks:

`file_thunk(path, mode) -> thunk`

> Returns a function which opens the file for you, when you need it.
> This decouples which file to open, and when to open/close it.
  
`withHandles2Thunk(src, dst, io) -> thunk`

> This is a "single attempt" function for performing an IO action with two resources.
> The resources can be any python context manager, like file handles, HTTP connections, or database sessions.
> The thunk ensures that both src and dst are properly cleaned up after the IO action, even an exception is raised.

`retry(thunk, predicate) -> thunk`

>       Call a thunk up to N times with backoff. Knows nothing about handles. Because retry takes a thunk,
>       it can be used to repeat any operation, including IO operations wrapped with withHandles2Thunk.

`pfmaplazy(fn, workers) -> iterator`

> Apply a function across an iterator of thread-workers. Useful for parallelizing IO-bound work. The `lazy` 
> part of the pfmaplazy means that the results are computed as needed instead of all at once, which allows
> you to defer the acquisition of resrources until they are actually needed.

`pipeline(*fns) -> functor`
> Pipeline composes multiple functions together, where the output of one function is the input to the next.
> This is a general-purpose function composition tool that can be used to build complex data processing
> pipelines from simple, reusable components. 
      
## Putting it all together:

These IO manager building blocks can compose together giving you the features you need a-la-carte with minimial code.
For example:

### Ideomitic file copy:

We want to copy a file from one place to another. But the normal way means we have to manage the lifecycle of both
handles. What do we do if an error occurs? Do we leak handles, or get to retry? When should we throw vs retry?

```python
def copy_file_normal(src_path: Path, dst_path: Path) -> None:
    with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
        copyfileobj(src, dst)
```

copy_file_normal looks simple, but its doinga lot. 

* Managing handle lifecycles.
* Performing the copy.
* Cleaning up resources on error.
* Deciding when to perform the copy (synchronously!)

We can break these apart. Lets make a function which just performs the IO action.

```python
def copy_file(src_fd: IO[bytes], dst_fd: IO[bytes]) -> None:
    return copyfileobj(src_fd, dst_fd)
```

`copy_file` has only one responsibility: copying data from src to dst.
It doesn't know anything about files, paths, or when to retry. It just does the copy.

We can restore our resource managment using withHandles2 and file_thunk!

```python
def copy_file_restored(src_path: Path, dst_path: Path) -> None:
    get_src = file_thunk(src_path, 'rb')
    get_dst = file_thunk(dst_path, 'wb')
    return withHandles2(get_src, get_dst, copy_file)
```

This looks like more code, but this pattern gives us more flexibility. Say that we didn't want to copy the file
synchromously, but instead defer it to later. We can do that by using withHandles2Thunk instead of withHandles2.

```python
def copy_file_deferred(src_path: Path, dst_path: Path) -> Callable[[], None]:
    get_src = file_thunk(src_path, 'rb')
    get_dst = file_thunk(dst_path, 'wb')
    return withHandles2Thunk(get_src, get_dst, copy_file)
```

copy_file_deferred returns a thunk that performs the copy when called. We can use this thunk with useful utilities
like retry to get automatic retries with backoff.

```python
# Define which exceptions we want to retry on.
is_copy_file_exception :: Exception -> bool
def is_copy_file_exception(e):
    return isinstance(e, IOError) # or whatever exceptions we want to retry on

def copy_file_with_retry(src_path: Path, dst_path: Path) -> None:
    return retry(copy_file_deferred(src_path, dst_path), is_copy_file_exception)
```

What if we want to copy a bunch of files? We could call copy_file_with_retry in a loop,
or we could convert it into a function which operates on an iteraable of Tuple[Path, Path].

```python
SRC_DST = Tuple[Path, Path]
copy_file_tuple: Callable[[SRC_DST], None] = uncurry(copy_file_with_retry)
```

We can use fmap to convert copy_file_tuple to operate on a list of tuples:

```python
copy_files: Callable[[Iterable[SRC_DST]], Iterable[None]] = fmap(copy_file_tuple)
```

`copy_files` can now be used top copy a list of files with automatic retries and proper resource management.
But we can do better! We can use pfmaplazy to copy files in parallel!

```python
parallel_copy_files: Callable[[Iterable[SRC_DST]], Iterable[None]] = pfmaplazy(copy_files, workers=4)
```

`pfmaplazy` will apply `copy_files` across the iterable of `SRC_DST` tuples using a pool of 4 worker threads.
Each copy operation will be retried with a backoff strategy and all resources will be properly managed,
even in the face of errors. Because pfmaplazy is lazy, the copy operations will apply backpressure so we
don't end up with too many open files or connections at once.

We can compose `parallel_copy_files` with other functions or IO operations using pipeline to build more complex 
data proessing pipelines.

```python
def copy_text_files(src_dir: Path, dst_dir: Path) -> Iterator[None]:
    is_text_file = lambda path: path.suffix == '.txt'
    dst_name = lambda path: dst_dir.join(path.name)
    src_dst_pair = lambda src: (src, dst_name(src))

    copy_txt_files_parallel = pipeline(
        ffilter(is_text_file),
        fmap(src_dst_pair),
        parallel_copy_files,
    )
    return copy_txt_files_parallel(src_dir)
```

When we are read to copy the files, we just read off the resulting iterator:

```python
# Use list to realize all the deffered operations.
results_iterator = copy_text_files("foo", "bar")
# ...
# force IO by consuming the iterator.
results = list(results_iterator)
```

All the resource managmenet, retries, error handeling can be delegated out of your core logic.