"""
Tests for the hyp_trees.py generator itself.

Verifies structural well-formedness of generated trees (root dir present,
every link has a csum, every dir has none) and that Hypothesis's shrinking
converges to a small counterexample on a deliberately-broken invariant.
These replace trees2.py's exhaustive-enumeration self-tests (no duplicate
trees, no csum-relabelling duplicates, exact shape/tree counts) which have
no meaning for a random generator -- there's no fixed universe to check
duplicates against, and "how many trees" isn't a property of a sampler.
"""

from hypothesis import given, settings

from farmfs.fs import DIR, LINK, ROOT
from tests.hyp_trees import (
    ALL_LINK_KINDS,
    BROKEN_KINDS,
    EXTERNAL_KINDS,
    EXTERNAL_ROOT_PLACEHOLDER,
    INTERNAL_KINDS,
    CONNECTED_KINDS,
    trees,
)


@given(tree=trees())
def test_every_tree_has_root_dir(tree):
    """Every tree must start with the root dir item."""
    assert len(tree) >= 1
    root_items = [i for i in tree if str(i["path"]) in ("/", ".") and i["type"] == DIR]
    assert len(root_items) == 1, f"Expected exactly one root dir, got {root_items} in {tree}"


@given(tree=trees())
def test_every_link_has_csum(tree):
    """With all link_kind flags off (the default), every link item is a
    blob-link and must have a non-None csum."""
    for item in tree:
        if item["type"] == LINK:
            assert item.get("csum") is not None, f"Link missing csum: {item}"


@given(tree=trees(
    include_interior_links=True, include_external_links=True, include_broken_links=True,
))
def test_every_link_has_exactly_one_kind(tree):
    """With every link_kind flag on, a link item is either a blob-link
    (csum set, link_kind None) or one of the 8 non-blob kinds (link_kind
    set, csum None) -- never both, never neither. This is the
    generator-side analogue of SnapshotItem's own "exactly one of
    csum/sub_path/rel_path" invariant."""
    for item in tree:
        if item["type"] == LINK:
            has_csum = item.get("csum") is not None
            has_kind = item.get("link_kind") is not None
            assert has_csum != has_kind, f"Link has {'both' if has_csum and has_kind else 'neither'}: {item}"
            if has_kind:
                assert item["link_kind"] in ALL_LINK_KINDS
                assert item.get("target") is not None
                # on_disk is deferred to materialization time for
                # relative-external kinds (see _on_disk_for's docstring in
                # hyp_trees.py) -- every other kind precomputes it.
                if item["link_kind"] not in ("rel_external_connected", "rel_external_broken"):
                    assert item.get("on_disk") is not None


@given(tree=trees())
def test_every_dir_has_no_csum(tree):
    """Every dir item must have csum=None."""
    for item in tree:
        if item["type"] == DIR:
            assert item.get("csum") is None, f"Dir has unexpected csum: {item}"


@given(tree=trees())
def test_no_duplicate_paths_within_a_tree(tree):
    """A single generated tree must never repeat a path."""
    paths = [str(item["path"]) for item in tree]
    assert len(paths) == len(set(paths)), f"Duplicate paths in tree: {tree}"


@given(tree=trees(max_nodes=8))
def test_node_budget_is_respected(tree):
    """max_nodes bounds the number of non-root nodes per draw."""
    non_root_count = len(tree) - 1
    assert non_root_count <= 8, f"Exceeded budget: {non_root_count} nodes in {tree}"


@given(tree=trees(
    include_interior_links=True, include_external_links=True, include_broken_links=True,
))
def test_broken_targets_never_collide(tree):
    """A broken-kind item's target (a ghost_N path) must never equal any
    other item's real path in the tree, nor any connected-external item's
    synthesized target -- ghost names use a naming convention (ghost_N)
    disjoint from both real node names (single letters) and external
    connected names (ext_N), so this should hold by construction; this
    test is the explicit regression guard for that guarantee."""
    real_paths = {str(item["path"]) for item in tree}
    other_targets = {
        str(item["target"]) for item in tree
        if item.get("link_kind") in CONNECTED_KINDS
    }
    for item in tree:
        if item.get("link_kind") in BROKEN_KINDS:
            target_str = str(item["target"])
            assert target_str not in real_paths, f"Broken target collides with a real path: {item}"
            assert target_str not in other_targets, f"Broken target collides with a connected target: {item}"


@given(tree=trees(include_interior_links=True, include_broken_links=True))
def test_internal_targets_are_contained_in_root(tree):
    """Every internal-kind item's target must resolve inside the
    generator's own abstract ROOT -- either ROOT itself or with ROOT among
    its parents."""
    for item in tree:
        if item.get("link_kind") in INTERNAL_KINDS:
            target = item["target"]
            assert target == ROOT or ROOT in target.parents(), f"Internal target escapes ROOT: {item}"


@given(tree=trees(include_external_links=True, include_broken_links=True))
def test_external_targets_are_not_contained_in_root(tree):
    """Every external-kind item's target must resolve under
    EXTERNAL_ROOT_PLACEHOLDER -- the synthetic stand-in for the shared
    external root -- never under any real tree node's path. (ROOT itself,
    Path("/"), is a literal ancestor of every absolute abstract path in
    this generator's convention, internal or external -- since depot root
    and filesystem root are the same abstract "/" until materialization
    rewrites internal paths onto vol_path -- so containment here is
    checked against the placeholder, not against ROOT.)"""
    real_paths = {item["path"] for item in tree}
    for item in tree:
        if item.get("link_kind") in EXTERNAL_KINDS:
            target = item["target"]
            assert (
                target == EXTERNAL_ROOT_PLACEHOLDER or EXTERNAL_ROOT_PLACEHOLDER in target.parents()
            ), f"External target is not under the placeholder root: {item}"
            assert target not in real_paths, f"External target collides with a real tree node: {item}"


@given(tree=trees(include_interior_links=True))
def test_connected_internal_target_is_a_real_node(tree):
    """A connected-internal link's target must appear as some other item's
    path in the same tree -- the explicit check on the DAG-pool guarantee
    from _items(), not just trusted from construction."""
    real_paths = {str(item["path"]) for item in tree}
    for item in tree:
        if item.get("link_kind") in ("abs_internal_connected", "rel_internal_connected"):
            assert str(item["target"]) in real_paths, f"Connected-internal target is not a tree node: {item}"


@given(tree=trees(
    include_interior_links=True, include_external_links=True, include_broken_links=True,
))
def test_relative_on_disk_string_is_correct_independent_of_relative_to(tree):
    """For every relative-kind item, hand-verify (split-and-count, not by
    calling Path.relative_to again -- that would just be testing itself)
    that walking on_disk's ".." segments up from the link's own parent,
    then descending the remaining segments, lands back on exactly the
    original target. This is the concrete regression guard for "escaping
    silently becomes internal": get the backtrack count or the trailing
    literal segment wrong and this reconstruction won't match target.
    """
    for item in tree:
        kind = item.get("link_kind")
        # rel_external_* defers on_disk to materialization time (see
        # _on_disk_for's docstring) -- nothing to hand-verify here yet.
        if kind not in ("rel_internal_connected", "rel_internal_broken"):
            continue
        link_path = item["path"]
        on_disk = item["on_disk"]
        target = item["target"]

        segments = on_disk.split("/")
        up_count = 0
        while up_count < len(segments) and segments[up_count] == "..":
            up_count += 1
        down_segments = segments[up_count:]

        cursor = link_path.parent()
        assert cursor is not None
        for _ in range(up_count):
            cursor = cursor.parent()
            assert cursor is not None, f"on_disk backtracks past filesystem root: {item}"
        for seg in down_segments:
            cursor = cursor.join(seg)

        assert cursor == target, f"Reconstructed target {cursor} != generated target {target}: {item}"


@given(tree=trees(include_external_links=True, include_broken_links=True))
@settings(deadline=None)
def test_relative_external_on_disk_string_is_correct_after_materialization(tmp_path_factory, tree):
    """The rel_external_* counterpart to
    test_relative_on_disk_string_is_correct_independent_of_relative_to:
    on_disk for these two kinds can't be hand-verified from the abstract
    tree alone (see that test and _on_disk_for's docstring) -- it's only
    known once build_tree() picks a real ext_root and computes it against
    real materialized paths. Materialize for real, then hand-verify
    (split-and-count, not calling relative_to again) that the resulting
    on-disk symlink string, read back off disk, actually resolves to the
    real external target -- the concrete regression guard for "escaping
    silently becomes internal" in the highest-risk case."""
    from farmfs.fs import Path
    from farmfs.volume import mkfs
    from tests.hyp_trees import build_tree

    has_external = any(item.get("link_kind") in EXTERNAL_KINDS for item in tree)
    if not has_external:
        return

    vol_dir = tmp_path_factory.mktemp("vol")
    ext_dir = tmp_path_factory.mktemp("external")
    vol_path = Path(str(vol_dir))
    ext_root = Path(str(ext_dir))
    udd = vol_path.join(".farmfs").join("userdata")
    mkfs(vol_path, udd)

    build_tree(vol_path, tree, ext_root=ext_root)

    for item in tree:
        kind = item.get("link_kind")
        if kind not in ("rel_external_connected", "rel_external_broken"):
            continue
        rel_str = str(item["path"]).lstrip("/")
        link_path = vol_path.join(rel_str)
        on_disk = link_path.readlink_raw()

        segments = on_disk.split("/")
        up_count = 0
        while up_count < len(segments) and segments[up_count] == "..":
            up_count += 1
        down_segments = segments[up_count:]

        cursor = link_path.parent()
        assert cursor is not None
        for _ in range(up_count):
            cursor = cursor.parent()
            assert cursor is not None, f"on_disk backtracks past filesystem root: {item}"
        for seg in down_segments:
            cursor = cursor.join(seg)

        ext_tail = str(item["target"])[len(str(EXTERNAL_ROOT_PLACEHOLDER)):].lstrip("/")
        expected = ext_root.join(ext_tail)
        assert cursor == expected, f"Reconstructed target {cursor} != real external target {expected}: {item}"


def test_shrinking_converges_on_a_broken_invariant():
    """
    Sanity check the harness itself: given a deliberately-wrong invariant
    (no tree may contain more than 2 nodes total), Hypothesis must find a
    failing example and shrink it to something minimal and legible -- not
    leave a large, hard-to-read counterexample.
    """
    import pytest

    captured = {}

    @given(tree=trees())
    @settings(max_examples=200, database=None)
    def _check(tree):
        captured["tree"] = tree  # overwritten on every call; last one is the shrunk minimal failure
        assert len(tree) <= 2

    with pytest.raises(AssertionError):
        _check()

    # The minimal way to violate "at most 2 nodes" is a 3-item tree (root +
    # 2 non-root nodes) -- shrinking must land on exactly that, not some
    # larger unshrunk example.
    assert len(captured["tree"]) == 3, f"Shrinking did not converge to a minimal example: {captured['tree']}"


def test_shrinking_converges_with_all_link_kinds_enabled():
    """Same pattern as test_shrinking_converges_on_a_broken_invariant, but
    redrawn with every link-kind flag on -- guards against the larger
    8-kind search space degrading shrink quality (e.g. shrinking landing
    on a needlessly large or illegible counterexample just because there
    are more strategy branches to explore)."""
    import pytest

    captured = {}

    @given(tree=trees(
        include_interior_links=True, include_external_links=True, include_broken_links=True,
    ))
    @settings(max_examples=200, database=None)
    def _check(tree):
        captured["tree"] = tree
        assert len(tree) <= 2

    with pytest.raises(AssertionError):
        _check()

    assert len(captured["tree"]) == 3, f"Shrinking did not converge to a minimal example: {captured['tree']}"
