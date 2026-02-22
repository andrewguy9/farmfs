from __future__ import annotations

from itertools import chain
from typing import Callable, Generator, Iterable, Iterator, List, Optional, TypeVar

import tqdm

from farmfs.util import cardinality, csum_pct

T = TypeVar("T")


def lazy_pbar(pbar_fn: Callable[[Iterable[T]], Generator[T, None, None]]) -> Callable[[Iterable[T]], Generator[T, None, None]]:
    """Defer tqdm construction until the first item is consumed.

    tqdm opens its bar in __init__ (at the top of the `with` block), before
    any items are pulled from the upstream iterator. When bars are nested
    inside a pipeline, this means the innermost bar opens first — the wrong
    order for display.

    Wrapping the inner bar with lazy_pbar causes it to pull one item from
    upstream before handing control to pbar_fn. That first pull opens the
    outer bar, then pbar_fn opens the inner bar — correct outer-to-inner order.
    """
    def _lazy(items: Iterable[T]) -> Generator[T, None, None]:
        it: Iterator[T] = iter(items)
        buf: List[T] = []
        for first in it:
            buf.append(first)
            break
        if not buf:
            return
        yield from pbar_fn(chain(buf, it))
    return _lazy


def pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable[[T], str]] = None,
    force_refresh: bool = False,
    total: Optional[int | float] = None,
    init_msg: Optional[str] = None,
    cardinality_fn: Optional[Callable[[int, T], int]] = None,
) -> Callable[[Iterable[T]], Generator[T, None, None]]:
    """General progress bar wrapper around tqdm.

    Args:
        label: Description label for the progress bar
        quiet: If True, disable progress bar output
        leave: If True, leave the progress bar on screen after completion
        postfix: Optional callable that takes an item and returns a string for postfix display
        force_refresh: If True, refresh display on every item (or at least on first item)
        total: Total count for the progress bar (None for unknown, float('inf') for infinite)
        init_msg: Initial message to display before iteration starts
        cardinality_fn: Optional callable that takes (index, item) and returns new total estimate
    """

    def _pbar(items: Iterable[T]) -> Generator[T, None, None]:
        with tqdm.tqdm(
            items,
            total=total,
            disable=quiet,
            leave=leave,
            desc=label,
        ) as pb:
            if init_msg:
                pb.set_postfix_str(init_msg, refresh=True)
                pb.update(0)
            prime = True
            for idx, item in enumerate(items, 1):
                refresh_now = prime or force_refresh
                if postfix is not None:
                    post_str = postfix(item)
                    pb.set_postfix_str(post_str, refresh=refresh_now)
                elif prime and init_msg:
                    pb.set_postfix_str("", refresh=True)
                elif refresh_now:
                    pb.refresh(nolock=False)
                prime = False
                yield item
                if pb.update(1) and cardinality_fn:
                    pb.total = cardinality_fn(idx, item)

    return _pbar


def list_pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable[[T], str]] = None,
    force_refresh: bool = False,
    total: Optional[int] = None,
) -> Callable[[Iterable[T]], Generator[T, None, None]]:
    """Progress bar for lists/sequences with known length.

    If total is provided it is used as the bar's count directly, allowing
    the source iterable to remain lazy. If omitted, tqdm will infer the
    total from the iterable (requires it to have __len__).
    """
    return pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=postfix,
        force_refresh=force_refresh,
        total=total,
        init_msg=f"Initializing {label}...",
    )


def csum_pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable[[str], str]] = None,
    force_refresh: bool = False,
) -> Callable[[Iterable[str]], Generator[str, None, None]]:
    """Progress bar for checksums with cardinality estimation."""

    def _postfix(csum: str) -> str:
        return postfix(csum) if postfix is not None else csum

    def _cardinality(idx: int, csum: str) -> int:
        pct = csum_pct(csum)
        return cardinality(idx, pct)

    return pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=_postfix,
        force_refresh=force_refresh,
        total=float("inf"),
        cardinality_fn=_cardinality,
    )


def tree_pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable[[T], str]] = None,
    force_refresh: bool = False,
) -> Callable[[Iterable[T]], Generator[T, None, None]]:
    """Progress bar for tree items with infinite total."""
    return pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=postfix,
        force_refresh=force_refresh,
        total=float("inf"),
    )
