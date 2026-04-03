from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Union

BlobPermissionKind = Literal["writable", "unreadable"]
KeydbProblemKind = Literal["legacy", "json_corrupt", "checksum_mismatch", "semantic"]


@dataclass
class MissingBlobIssue:
    kind: Literal["missing_blob"] = "missing_blob"
    csum: str = ""
    snap: str = ""
    path: str = ""
    is_fixed: Optional[bool] = None


@dataclass
class FrozenIgnoredIssue:
    kind: Literal["frozen_ignored"] = "frozen_ignored"
    path: str = ""
    is_fixed: Optional[bool] = None


@dataclass
class BadPermissionsIssue:
    kind: Literal["bad_permissions"] = "bad_permissions"
    csum: str = ""
    permission_issue: BlobPermissionKind = "writable"
    is_fixed: Optional[bool] = None


@dataclass
class ChecksumMismatchIssue:
    kind: Literal["checksum_mismatch"] = "checksum_mismatch"
    expected_csum: str = ""
    actual_csum: str = ""
    is_fixed: Optional[bool] = None


@dataclass
class KeydbIssue:
    kind: Literal["keydb_issue"] = "keydb_issue"
    key: str = ""
    problem: KeydbProblemKind = "legacy"
    detail: str = ""
    is_fixed: Optional[bool] = None


FsckIssue = Union[MissingBlobIssue, FrozenIgnoredIssue, BadPermissionsIssue, ChecksumMismatchIssue, KeydbIssue]


def encode_fsck_issue(i: FsckIssue) -> Dict[str, Any]:
    if isinstance(i, MissingBlobIssue):
        d: Dict[str, Any] = {"kind": i.kind, "csum": i.csum, "snap": i.snap, "path": i.path}
    elif isinstance(i, FrozenIgnoredIssue):
        d = {"kind": i.kind, "path": i.path}
    elif isinstance(i, BadPermissionsIssue):
        d = {"kind": i.kind, "csum": i.csum, "permission_issue": i.permission_issue}
    elif isinstance(i, ChecksumMismatchIssue):
        d = {"kind": i.kind, "expected_csum": i.expected_csum, "actual_csum": i.actual_csum}
    else:
        d = {"kind": i.kind, "key": i.key, "problem": i.problem, "detail": i.detail}
    if i.is_fixed is not None:
        d["is_fixed"] = i.is_fixed
    return d


def format_fsck_issue(i: FsckIssue) -> str:
    """Format an FsckIssue as a single human-readable line for text output."""
    suffix = ""
    if i.is_fixed is True:
        suffix = "  fixed=true"
    elif i.is_fixed is False:
        suffix = "  fixed=false"

    if isinstance(i, MissingBlobIssue):
        return f"missing_blob  {i.csum}  snap={i.snap}  path={i.path}{suffix}"
    elif isinstance(i, FrozenIgnoredIssue):
        return f"frozen_ignored  path={i.path}{suffix}"
    elif isinstance(i, BadPermissionsIssue):
        fix_detail = ""
        if i.is_fixed is False:
            fix_detail = "  (cannot fix: not owner)"
        return f"bad_permissions  {i.csum}  {i.permission_issue}{suffix}{fix_detail}"
    elif isinstance(i, ChecksumMismatchIssue):
        return f"checksum_mismatch  expected={i.expected_csum}  actual={i.actual_csum}{suffix}"
    else:
        detail_part = f"  detail={i.detail}" if i.detail else ""
        return f"keydb_issue  key={i.key}  problem={i.problem}{detail_part}{suffix}"
