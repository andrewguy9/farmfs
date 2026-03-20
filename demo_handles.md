# Managed IO Pipelines

How to get automatic retries, resource reuse and parallelism without leaking
connections or complicating your code.

## Strategy

Don't try to manage connections, sockets, and file handles directly.
Instead separate operation description from operation invocation using *thunks*.

Then you can use side-effect management functions which can take responsibility
for acquiring and releasing these limited resources for you.

IO bracketing functions can yield other benefits like automatic retries,
connection pooling/reuse, and parallelism with much less effort and fewer resource leak bugs.

## The building blocks

`file_thunk(path, mode) -> thunk`

> Returns a function which opens the file for you, when you need it.
> This decouples which file to open, and when to open/close it.

`withHandles2Thunk(src, dst, io) -> thunk`

> Returns a "single attempt" function for performing an IO action with two resources.
> The resources can be any Python context manager, like file handles, HTTP connections, or database sessions.
> The wrapper ensures that both src and dst are properly cleaned up after the IO action, even if an exception is raised.

`retry(thunk, predicate) -> result`

> Call a thunk up to N times with backoff. Knows nothing about handles.

`retryThunk(thunk, predicate) -> thunk`

> Deferred version of retry — returns a thunk that, when called, retries up to N times with backoff.
> Because retryThunk returns a thunk, it composes naturally with pfmaplazy and other combinators.

`pfmaplazy(fn, workers) -> iterator`

> Apply a function across an iterator of thread-workers. Useful for parallelizing IO-bound work. The `lazy`
> part of the pfmaplazy means that the results are computed as needed instead of all at once, which allows
> you to defer the acquisition of resources until they are actually needed.

`pipeline(*fns) -> callable`

> Pipeline composes multiple functions together, where the output of one function is the input to the next.
> This is a general-purpose function composition tool that can be used to build complex data processing
> pipelines from simple, reusable components.

## Putting it all together

These IO manager building blocks can compose together giving you the features you need a-la-carte with minimal code.
For example:

### Idiomatic file copy

We want to copy a file from one place to another. But the normal way means we have to manage the lifecycle of both
handles. What do we do if an error occurs? Do we leak handles, or get to retry? When should we throw vs retry?

```python
def copy_file_normal(src_path: Path, dst_path: Path) -> None:
    with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
        copyfileobj(src, dst)
```

`copy_file_normal` looks simple, but it's doing a lot.

* Managing handle lifecycles.
* Performing the copy.
* Cleaning up resources on error. (via with)
* Propogating errors (throw everything!)
* Deciding when to perform the copy (synchronously!)

We can break these concerns apart! 

First, make a function which just performs the IO action.

```python
def copy_file(src_fd: IO[bytes], dst_fd: IO[bytes]) -> None:
    copyfileobj(src_fd, dst_fd)
```

`copy_file` has only one responsibility: copying data from src to dst.
It doesn't know anything about files, paths, or when to retry. It just does the copy.

Second, we need functions to take responsibility for acquiring the src and dst files.

```python
get_src = file_thunk(src_path, 'rb')
get_dst = file_thunk(dst_path, 'wb')
```

Combining these tools, we reimplement `copy_file_normal`'s behavior:

```python
def copy_file_restored(src_path: Path, dst_path: Path) -> None:
    get_src = file_thunk(src_path, 'rb')
    get_dst = file_thunk(dst_path, 'wb')
    return withHandles2(get_src, get_dst, copy_file)
```

While this is more code than before, using the *thunk* pattern gives us more flexibility. Say that we didn't want to copy the file
synchronously, but instead defer it to later. We can do that by using `withHandles2Thunk` instead of `withHandles2`.

```python
def copy_file_deferred(src_path: Path, dst_path: Path) -> Callable[[], None]:
    get_src = file_thunk(src_path, 'rb')
    get_dst = file_thunk(dst_path, 'wb')
    return withHandles2Thunk(get_src, get_dst, copy_file)

do_copy = copy_file_deferred("foo.txt", "bar.txt")
# ...
result = do_copy()
```

`copy_file_deferred` returns a thunk that performs the copy when called. We can add automatic retries
with backoff using `retryThunk`, which wraps a thunk with retry logic and returns a new thunk:

```python
# Define which exceptions we want to retry on.
def is_copy_file_exception(e: Exception) -> bool:
    return isinstance(e, IOError) # or whatever exceptions we want to retry on

def copy_file_with_retry(src_path: Path, dst_path: Path) -> Callable[[], None]:
    return retryThunk(copy_file_deferred(src_path, dst_path), is_copy_file_exception)

do_copy = copy_file_with_retry("foo.txt", "bar.txt")
# ...
do_copy()  # executes with retries
```

Because `retryThunk` returns a thunk (just like `withHandles2Thunk` and `copy_file_deferred`),
the result stays deferred until we explicitly call it.

What if we want to copy a bunch of files? We could call `copy_file_with_retry` in a loop,
or we could convert it into a function which operates on an iterable of `Tuple[Path, Path]`.
`uncurry` unpacks a tuple into positional arguments, so a function that takes `src, dst`
can accept a single `(src, dst)` tuple instead.

```python
SRC_DST = Tuple[Path, Path]
copy_file_tuple: Callable[[SRC_DST], None] = uncurry(copy_file_with_retry)

pair = ("foo.txt", "bar.txt")
copy_file_tuple(pair)
```

We can use fmap to convert copy_file_tuple to operate on a list of tuples:

```python
copy_files: Callable[[Iterable[SRC_DST]], Iterable[None]] = fmap(copy_file_tuple)

pairs = [("foo.txt", "bar.txt"), ("car.txt", "far.txt")]
doers = copy_files(pairs)
# ...
results = list(doers)
```

`copy_files` can now be used to copy a list of files with automatic retries and proper resource management.
But we can do better! We can use pfmaplazy to copy files in parallel!

```python
parallel_copy_files: Callable[[Iterable[SRC_DST]], Iterable[None]] = pfmaplazy(copy_file_tuple, workers=4)

pairs = [("foo.txt", "bar.txt"), ("car.txt", "far.txt")]
results_iter = parallel_copy_files(pairs)
# ...
results = list(results_iter)
```

`pfmaplazy` will apply `copy_file_tuple` across the iterable of `SRC_DST` tuples using a pool of 4 worker threads.
Each copy operation will be retried with a backoff strategy and all resources will be properly managed,
even in the face of errors. Because pfmaplazy is lazy, the copy operations will apply backpressure so we
don't end up with too many open files or connections at once.

We can compose `parallel_copy_files` with other functions or IO operations using pipeline to build more complex
data processing pipelines.

```python
def copy_text_files(src_dir: Path, dst_dir: Path) -> Iterator[None]:
    is_text_file = lambda path: path.suffix == '.txt'
    dst_name = lambda path: dst_dir.join(path.name)
    src_dst_pair = lambda src: (src, dst_name(src))

    copy_txt_files_parallel = pipeline(
        ffilter(is_text_file),  # Just process txt files
        fmap(src_dst_pair),     # Make SRC_DST pairs
        parallel_copy_files,    # Copy files
    )
    return copy_txt_files_parallel(src_dir)

results_iter = copy_text_files("foo", "bar")
# ...
results = list(results_iter)
```
All the resource management, retries, and error handling can be delegated out of your core logic.