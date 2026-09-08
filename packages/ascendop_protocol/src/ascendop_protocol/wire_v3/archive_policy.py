"""Common member policy for C6 payload readers; no transport or state ownership."""
from __future__ import annotations

import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import tarfile


MAX_MEMBERS = 100_000
MAX_EXPANDED_BYTES = 4 * 1024 * 1024 * 1024


def member_path(name: str, *, error_type: type[Exception] = ValueError) -> PurePosixPath:
    relative = PurePosixPath(name)
    windows = PureWindowsPath(name)
    if (not name or "\\" in name or ":" in name or windows.drive or windows.root
            or relative.is_absolute() or ".." in relative.parts):
        raise error_type(f"payload archive path escapes destination (unsafe path): {name}")
    return relative


def validate_members(
    archive: tarfile.TarFile,
    destination: Path,
    *,
    error_type: type[Exception] = ValueError,
    max_members: int = MAX_MEMBERS,
    max_expanded_bytes: int = MAX_EXPANDED_BYTES,
) -> list[tarfile.TarInfo]:
    """Validate the complete bounded inventory before a caller writes any member.

    Normalize aliases before duplicate detection. A root directory entry is
    allowed for normal tar producers, but a root file is never allowed.
    Extraction remains with each caller, preserving its publication protocol.
    """
    destination = destination.resolve()
    members = []
    seen = set()
    files = set()
    directories = set()
    expanded = 0
    for member in archive:
        if len(members) >= max_members:
            raise error_type(f"payload archive exceeds {max_members} members")
        relative = member_path(member.name, error_type=error_type)
        target = destination.joinpath(*relative.parts).resolve()
        if target != destination and destination not in target.parents:
            raise error_type(f"payload archive path escapes destination: {member.name}")
        identity = os.path.normcase(str(target))
        if identity in seen:
            raise error_type(f"payload archive duplicate member: {member.name}")
        seen.add(identity)
        if member.issym() or member.islnk():
            raise error_type(f"payload archive links are forbidden (unsupported member): {member.name}")
        if not member.isfile() and not member.isdir():
            raise error_type(f"payload archive member type is forbidden (unsupported member): {member.name}")
        parents = [os.path.normcase(str(parent)) for parent in target.parents
                   if parent == destination or destination in parent.parents]
        if any(parent in files for parent in parents) or (member.isfile() and identity in directories):
            raise error_type(f"payload archive file/directory conflict: {member.name}")
        directories.update(parents)
        if member.isfile():
            if target == destination or member.size < 0:
                raise error_type(f"payload archive invalid file member: {member.name}")
            expanded += member.size
            if expanded > max_expanded_bytes:
                raise error_type(f"expanded payload exceeds {max_expanded_bytes} bytes")
            files.add(identity)
        else:
            directories.add(identity)
        member.mode &= 0o777
        member.uid = member.gid = -1
        member.uname = member.gname = ""
        members.append(member)
    return members
