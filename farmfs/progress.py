from __future__ import annotations

from typing import Callable, Generator, Iterable, Optional

import tqdm

from farmfs.util import cardinality, csum_pct


def pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable] = None,
    force_refresh: bool = False,
    position: Optional[int] = None,
    total: Optional[int | float] = None,
    init_msg: Optional[str] = None,
    cardinality_fn: Optional[Callable] = None,
) -> Callable[[Iterable], Generator]:
    """General progress bar wrapper around tqdm.

    Args:
        label: Description label for the progress bar
        quiet: If True, disable progress bar output
        leave: If True, leave the progress bar on screen after completion
        postfix: Optional callable that takes an item and returns a string for postfix display
        force_refresh: If True, refresh display on every item (or at least on first item)
        position: Vertical position for nested progress bars (tqdm position parameter)
        total: Total count for the progress bar (None for unknown, float('inf') for infinite)
        init_msg: Initial message to display before iteration starts
        cardinality_fn: Optional callable that takes (index, item) and returns new total estimate
    """

    def _pbar(items: Iterable) -> Generator:
        with tqdm.tqdm(
            items,
            total=total,
            disable=quiet,
            leave=leave,
            desc=label,
            position=position,
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
    postfix: Optional[Callable] = None,
    force_refresh: bool = False,
    position: Optional[int] = None,
) -> Callable[[Iterable], Generator]:
    """Progress bar for lists/sequences with known length."""
    return pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=postfix,
        force_refresh=force_refresh,
        position=position,
        init_msg=f"Initializing {label}...",
    )


def csum_pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable[[str], str]] = None,
    force_refresh: bool = False,
    position: Optional[int] = None,
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
        position=position,
        total=float("inf"),
        cardinality_fn=_cardinality,
    )


def tree_pbar(
    label: str = "",
    quiet: bool = False,
    leave: bool = True,
    postfix: Optional[Callable] = None,
    force_refresh: bool = False,
    position: Optional[int] = None,
) -> Callable[[Iterable], Generator]:
    """Progress bar for tree items with infinite total."""
    return pbar(
        label=label,
        quiet=quiet,
        leave=leave,
        postfix=postfix,
        force_refresh=force_refresh,
        position=position,
        total=float("inf"),
    )
