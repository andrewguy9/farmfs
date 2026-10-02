"""
Hypothesis-based tree generator for snapshot/diff/patch tests.

Replaces the exhaustive canonicalization-based generator in trees2.py
(shapes + restricted-growth-string csum partitions) with random generation
plus automatic shrinking. Step 1 of the migration reproduced exactly the
old coverage -- a rooted tree of dirs and blob-links only. Step 3
incrementally adds new link kinds; see
/Users/andrewthomson/.claude/plans/lexical-sniffing-horizon.md for the plan.

Interior/exterior symlink generation (2026-09): three independent axes fully
describe the space of non-blob symlinks a real tree can contain:
  - relative vs absolute on-disk target string (decides rel_path vs
    sub_path in the production classification, see farmfs/snapshot.py's
    classify_link())
  - connected vs broken (does the target actually exist on disk)
  - internal vs external (does the target resolve inside the depot root,
    or outside it -- external must always be rejected by farmfs)
That's 8 leaf kinds, gated behind three independent opt-in flags on
trees(): include_interior_links, include_external_links,
include_broken_links. All default False, so trees()'s default output is
unchanged: dirs and blob-links only, exactly today's original coverage.

External targets don't have a real filesystem location at draw time (the
generator produces abstract trees, materialized later against whatever
tmp_path_factory hands the caller) -- EXTERNAL_ROOT_PLACEHOLDER stands in
for "wherever the shared external root ends up," and build_tree() rewrites
it onto the real external root at materialization time. This mirrors how
internal paths were already abstract, rooted at the literal ROOT = Path("/")
from farmfs.fs, joined against a real volume only at materialization.

Public API
----------
trees(max_nodes=8, include_interior_links=False, include_external_links=False,
      include_broken_links=False) -> SearchStrategy[List[Dict]]
    Each draw produces a tree encoded as a flat list of item dicts:
        {
            "path": Path,             # abstract path under ROOT
            "type": LINK | DIR,
            "csum": str | None,       # blob-link class index, else None
            "link_kind": str | None,  # one of the 8 kinds below, else None
            "on_disk": str | None,    # exact string for the symlink
            "target": Path | None,    # resolved abstract target
        }

    link_kind is one of: abs_internal_connected, rel_internal_connected,
    abs_external_connected, rel_external_connected, abs_internal_broken,
    rel_internal_broken, abs_external_broken, rel_external_broken.

    The csum for blob-links is a decimal string of a small class index
    ("0", "1", ...), matching trees2.py's convention -- callers needing a
    real MD5 digest still call csum_bytes(int(item["csum"])) themselves.

build_tree(vol_path, tree, ext_root=None) -> None
    Materializes a tree (as produced by trees()) into a real directory.
    ext_root is a real Path to use for external-kind items' targets; pass
    None if the tree has no external items (the common case).

Callers use trees() directly with @given(tree=trees()) rather than pytest
parametrize -- Hypothesis's own execution model (many calls per test
function, shrinking on failure) doesn't fit the old fixture-based wiring.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from hypothesis import strategies as st

from farmfs.fs import DIR, LINK, ROOT, Path, ensure_symlink_unsafe
from tests.conftest import build_blob, build_dir, build_link

ALPHABET = list("abcdefghijklmnopqrstuvwxyz")

# Small, fixed pool of csum classes -- mirrors trees2.py's restricted-growth
# style coverage (repeated classes exercise dedup) without needing canonical
# partition enumeration, which has no meaning for a random generator.
MAX_CSUM_CLASSES = 4

# Abstract placeholder for "wherever the shared external root ends up" --
# never a real filesystem path. build_tree() rewrites it onto the real
# ext_root it's given at materialization time.
EXTERNAL_ROOT_PLACEHOLDER = Path("/__farmfs_test_external__")

# link_kind tags for the 8 combinations.
ABS_INTERNAL_CONNECTED = "abs_internal_connected"
REL_INTERNAL_CONNECTED = "rel_internal_connected"
ABS_EXTERNAL_CONNECTED = "abs_external_connected"
REL_EXTERNAL_CONNECTED = "rel_external_connected"
ABS_INTERNAL_BROKEN = "abs_internal_broken"
REL_INTERNAL_BROKEN = "rel_internal_broken"
ABS_EXTERNAL_BROKEN = "abs_external_broken"
REL_EXTERNAL_BROKEN = "rel_external_broken"

INTERNAL_KINDS = (ABS_INTERNAL_CONNECTED, REL_INTERNAL_CONNECTED, ABS_INTERNAL_BROKEN, REL_INTERNAL_BROKEN)
EXTERNAL_KINDS = (ABS_EXTERNAL_CONNECTED, REL_EXTERNAL_CONNECTED, ABS_EXTERNAL_BROKEN, REL_EXTERNAL_BROKEN)
BROKEN_KINDS = (ABS_INTERNAL_BROKEN, REL_INTERNAL_BROKEN, ABS_EXTERNAL_BROKEN, REL_EXTERNAL_BROKEN)
CONNECTED_KINDS = (ABS_INTERNAL_CONNECTED, REL_INTERNAL_CONNECTED, ABS_EXTERNAL_CONNECTED, REL_EXTERNAL_CONNECTED)
ALL_LINK_KINDS = INTERNAL_KINDS + EXTERNAL_KINDS


class _Counters:
    """Monotonic counters for ghost/external names -- uniqueness is the
    only property needed here (never collide with a real node or with each
    other), so these are plain counters closed over during generation, not
    drawn from Hypothesis. Shrink-search diversity on these names would add
    search space with no coverage benefit."""

    def __init__(self) -> None:
        self.ghost = 0
        self.ext = 0

    def next_ghost(self) -> str:
        self.ghost += 1
        return "ghost_%d" % self.ghost

    def next_ext(self) -> str:
        self.ext += 1
        return "ext_%d" % self.ext


def _on_disk_for(kind: str, target: Path, link_path: Path) -> Optional[str]:
    """
    The exact on-disk symlink target string for a given kind/target, or
    None if it can't be known until materialization.

    Absolute kinds and relative-INTERNAL kinds can be precomputed here:
    target and link_path are both abstract paths under the same abstract
    ROOT, so the backtrack arithmetic and the literal string are both
    already correct as generated -- internal targets never get rewritten
    at materialization time.

    Relative-EXTERNAL kinds cannot: target is expressed against
    EXTERNAL_ROOT_PLACEHOLDER, a fake root with no relationship to
    link_path's real eventual location, so the number of ".." segments
    computed here would be right (both paths are still under the same
    abstract filesystem-root ancestor) but the trailing literal segment
    ("__farmfs_test_external__/ext_N") would be wrong -- it needs to name
    the REAL external root's path relative to the link, which isn't known
    until build_tree() picks a real ext_root. Those two kinds compute
    on_disk at materialization time instead (see build_tree()).
    """
    if kind in (ABS_INTERNAL_CONNECTED, ABS_EXTERNAL_CONNECTED, ABS_INTERNAL_BROKEN, ABS_EXTERNAL_BROKEN):
        return str(target)
    if kind in (REL_EXTERNAL_CONNECTED, REL_EXTERNAL_BROKEN):
        return None
    parent = link_path.parent()
    assert parent is not None
    return target.relative_to(parent)


@st.composite
def _items(
    draw,
    budget: int,
    rel_path: str,
    pool: List[Path],
    counters: _Counters,
    include_interior_links: bool,
    include_external_links: bool,
    include_broken_links: bool,
) -> List[Dict]:
    """
    Draw a dir's children as a flat list of item dicts, consuming at most
    `budget` nodes total. `pool` is the list of absolute Paths already
    placed earlier in this generation pass (ancestors, earlier siblings,
    and everything already placed inside them) -- an internal-connected
    link may only target something in `pool`, which guarantees the target
    graph is a DAG (a link can never target itself or anything placed
    later).

    pool is mutated in place as items are placed, so later siblings (and
    this dir's own later children) see everything placed so far.
    """
    if budget <= 0:
        return []
    count = draw(st.integers(min_value=0, max_value=min(budget, len(ALPHABET))))
    items: List[Dict] = []
    left = budget
    for i in range(count):
        if left <= 0:
            break
        name = ALPHABET[i]
        child_path_str = rel_path.rstrip("/") + "/" + name
        child_path = Path(child_path_str)

        kinds = ["dir", "blob"]
        if left > 1 and include_interior_links and pool:
            kinds.append("interior_link")
        if left > 1 and include_external_links:
            kinds.append("external_link")
        if left > 1 and include_broken_links and (include_interior_links or include_external_links):
            kinds.append("broken_link")
        kind = draw(st.sampled_from(kinds))

        if kind == "dir":
            sub_items = draw(_items(
                left - 1, child_path_str, pool, counters,
                include_interior_links, include_external_links, include_broken_links,
            ))
            items.append({
                "path": child_path, "type": DIR, "csum": None,
                "link_kind": None, "on_disk": None, "target": None,
            })
            pool.append(child_path)
            items.extend(sub_items)
            left -= 1 + len(sub_items)
        elif kind == "blob":
            csum_class = draw(st.integers(min_value=0, max_value=MAX_CSUM_CLASSES - 1))
            items.append({
                "path": child_path, "type": LINK, "csum": str(csum_class),
                "link_kind": None, "on_disk": None, "target": None,
            })
            pool.append(child_path)
            left -= 1
        elif kind == "interior_link":
            target = draw(st.sampled_from(pool))
            is_rel = draw(st.booleans())
            link_kind = REL_INTERNAL_CONNECTED if is_rel else ABS_INTERNAL_CONNECTED
            on_disk = _on_disk_for(link_kind, target, child_path)
            items.append({
                "path": child_path, "type": LINK, "csum": None,
                "link_kind": link_kind, "on_disk": on_disk, "target": target,
            })
            pool.append(child_path)
            left -= 1
        elif kind == "external_link":
            target = Path(counters.next_ext(), EXTERNAL_ROOT_PLACEHOLDER)
            is_rel = draw(st.booleans())
            link_kind = REL_EXTERNAL_CONNECTED if is_rel else ABS_EXTERNAL_CONNECTED
            on_disk = _on_disk_for(link_kind, target, child_path)
            items.append({
                "path": child_path, "type": LINK, "csum": None,
                "link_kind": link_kind, "on_disk": on_disk, "target": target,
            })
            pool.append(child_path)
            left -= 1
        else:  # broken_link
            # A broken link's internal/external axis is independently
            # gated too -- include_broken_links alone must only produce
            # broken kinds whose other axis (internal vs external) is
            # itself enabled, exactly like the connected kinds above.
            can_internal = include_interior_links and bool(pool)
            can_external = include_external_links
            if can_internal and can_external:
                is_internal = draw(st.booleans())
            elif can_internal:
                is_internal = True
            else:
                is_internal = False
            is_rel = draw(st.booleans())
            if is_internal:
                ghost_parent = draw(st.sampled_from(pool))
                link_kind = REL_INTERNAL_BROKEN if is_rel else ABS_INTERNAL_BROKEN
            else:
                ghost_parent = EXTERNAL_ROOT_PLACEHOLDER
                link_kind = REL_EXTERNAL_BROKEN if is_rel else ABS_EXTERNAL_BROKEN
            target = Path(counters.next_ghost(), ghost_parent)
            on_disk = _on_disk_for(link_kind, target, child_path)
            items.append({
                "path": child_path, "type": LINK, "csum": None,
                "link_kind": link_kind, "on_disk": on_disk, "target": target,
            })
            pool.append(child_path)
            left -= 1
    return items


@st.composite
def _tree(
    draw,
    max_nodes: int,
    include_interior_links: bool,
    include_external_links: bool,
    include_broken_links: bool,
) -> List[Dict]:
    pool: List[Path] = [ROOT]
    counters = _Counters()
    items = [{"path": ROOT, "type": DIR, "csum": None, "link_kind": None, "on_disk": None, "target": None}]
    items.extend(draw(_items(
        max_nodes, "/", pool, counters,
        include_interior_links, include_external_links, include_broken_links,
    )))
    return items


def trees(
    max_nodes: int = 8,
    include_interior_links: bool = False,
    include_external_links: bool = False,
    include_broken_links: bool = False,
) -> "st.SearchStrategy[List[Dict]]":
    """
    Strategy producing trees of dirs and blob-links (today's default
    coverage), plus the 8 interior/exterior symlink kinds when the
    corresponding flags are enabled -- see module docstring. max_nodes
    bounds the number of non-root nodes per draw.
    """
    return _tree(max_nodes, include_interior_links, include_external_links, include_broken_links)


def csum_bytes(class_idx: int) -> bytes:
    """Map a class index to a deterministic bytes value for MD5 hashing."""
    return str(class_idx).encode()


def _rel(path: Path) -> str:
    s = str(path)
    return s.lstrip("/")


def build_tree(vol_path: Path, tree: list, ext_root: Optional[Path] = None) -> None:
    """
    Materialize a tree (as produced by trees()) into a real volume rooted
    at vol_path. ext_root is a real Path to place external-kind items'
    targets under -- required if the tree contains any external-kind item,
    ignored otherwise. Items are materialized in the order given, which is
    always draw order, which is always "target already materialized before
    anything that targets it" (guaranteed by the pool mechanics in
    _items()).
    """
    built: Dict[str, Path] = {"/": vol_path}

    def rewrite(p: Path) -> Path:
        """Rewrite an abstract target Path onto a real filesystem Path --
        EXTERNAL_ROOT_PLACEHOLDER prefix maps onto the real ext_root,
        anything else is an internal abstract path under ROOT, mapped onto
        vol_path the same way _rel()+join always has been."""
        s = str(p)
        placeholder_str = str(EXTERNAL_ROOT_PLACEHOLDER)
        if s == placeholder_str or s.startswith(placeholder_str + "/"):
            assert ext_root is not None, "tree has an external-kind item but no ext_root was given"
            tail = s[len(placeholder_str):].lstrip("/")
            return Path(tail, ext_root) if tail else ext_root
        return Path(_rel(p), vol_path)

    for item in tree:
        rel = item["path"]
        if str(rel) in ("/", "."):
            continue
        rel_str = _rel(rel)
        if item["type"] == DIR:
            p = build_dir(vol_path, rel_str)
            built[str(rel)] = p
        elif item["type"] == LINK and item.get("link_kind") is None:
            content = csum_bytes(int(item["csum"]))
            real_csum = build_blob(vol_path, content)
            p = build_link(vol_path, rel_str, real_csum)
            built[str(rel)] = p
        else:
            link_kind = item["link_kind"]
            link_path = Path(rel_str, vol_path)
            real_target = rewrite(item["target"])

            if link_kind in (ABS_EXTERNAL_CONNECTED, REL_EXTERNAL_CONNECTED):
                # External-connected: create a trivial file at the real
                # target before linking to it.
                with real_target.open("w") as fd:
                    fd.write("external")

            if link_kind in (ABS_INTERNAL_CONNECTED, ABS_EXTERNAL_CONNECTED, ABS_INTERNAL_BROKEN, ABS_EXTERNAL_BROKEN):
                link_path.symlink(real_target)
            elif link_kind in (REL_INTERNAL_CONNECTED, REL_INTERNAL_BROKEN):
                # Relative-internal: on_disk was computed against the
                # abstract target/link_path at draw time and is correct
                # verbatim -- both sides are abstract paths under the same
                # abstract root, so the backtrack arithmetic transfers
                # unchanged to wherever the tree actually materializes.
                ensure_symlink_unsafe(link_path, item["on_disk"])
            else:
                # Relative-external: on_disk couldn't be precomputed (see
                # _on_disk_for's docstring) since it depends on the real
                # ext_root's location relative to the real link_path,
                # neither of which exists until now. Compute it here
                # against the real materialized paths.
                parent = link_path.parent()
                assert parent is not None
                on_disk = real_target.relative_to(parent)
                ensure_symlink_unsafe(link_path, on_disk)
            built[str(rel)] = link_path
