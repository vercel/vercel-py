"""An in-memory POSIX-shaped filesystem for fake sandbox sessions."""

from __future__ import annotations

import os
import posixpath
import shutil
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

DEFAULT_CWD = "/vercel/sandbox"


class FilesystemError(Exception):
    """A filesystem operation failed the way the sandbox would report it."""

    def __init__(self, code: Literal["not_found", "exists", "is_directory", "not_directory"]):
        super().__init__(code)
        self.code = code


@dataclass
class File:
    data: bytes
    mode: int = 0o644


@dataclass
class Directory:
    mode: int = 0o755


Node = File | Directory


@dataclass
class FakeFilesystem:
    """Absolute paths mapped to files and directories.

    Only what the SDK can observe is modelled: bytes, permission bits, and
    the file/directory distinction. There are no links, owners, or times.
    """

    nodes: dict[str, Node] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.nodes.setdefault("/", Directory())
        self.mkdir(DEFAULT_CWD, parents=True, exist_ok=True)

    @staticmethod
    def resolve(cwd: str, path: str) -> str:
        return posixpath.normpath(posixpath.join(cwd, path)).replace("//", "/")

    def copy(self) -> FakeFilesystem:
        return FakeFilesystem(
            nodes={
                path: File(node.data, node.mode) if isinstance(node, File) else Directory(node.mode)
                for path, node in self.nodes.items()
            }
        )

    def exists(self, path: str) -> bool:
        return path in self.nodes

    def is_file(self, path: str) -> bool:
        return isinstance(self.nodes.get(path), File)

    def is_dir(self, path: str) -> bool:
        return isinstance(self.nodes.get(path), Directory)

    def read(self, path: str) -> bytes:
        node = self.nodes.get(path)
        if node is None:
            raise FilesystemError("not_found")
        if isinstance(node, Directory):
            raise FilesystemError("is_directory")
        return node.data

    def mode(self, path: str) -> int:
        node = self.nodes.get(path)
        if node is None:
            raise FilesystemError("not_found")
        return node.mode

    def write(self, path: str, data: bytes, *, mode: int | None = None) -> None:
        if isinstance(self.nodes.get(path), Directory):
            raise FilesystemError("is_directory")
        self.mkdir(posixpath.dirname(path), parents=True, exist_ok=True)
        previous = self.nodes.get(path)
        default_mode = previous.mode if isinstance(previous, File) else 0o644
        self.nodes[path] = File(data, default_mode if mode is None else mode)

    def mkdir(self, path: str, *, parents: bool = False, exist_ok: bool = False) -> None:
        if path in self.nodes:
            if isinstance(self.nodes[path], File):
                raise FilesystemError("not_directory")
            if not exist_ok:
                raise FilesystemError("exists")
            return
        parent = posixpath.dirname(path)
        if parent not in self.nodes:
            if not parents:
                raise FilesystemError("not_found")
            self.mkdir(parent, parents=True, exist_ok=True)
        elif isinstance(self.nodes[parent], File):
            raise FilesystemError("not_directory")
        self.nodes[path] = Directory()

    def listdir(self, path: str) -> list[tuple[str, Literal["file", "directory"]]]:
        if not self.is_dir(path):
            raise FilesystemError("not_directory" if path in self.nodes else "not_found")
        prefix = path.rstrip("/") + "/"
        entries: list[tuple[str, Literal["file", "directory"]]] = []
        for candidate, node in sorted(self.nodes.items()):
            if candidate == path or not candidate.startswith(prefix):
                continue
            name = candidate[len(prefix) :]
            if "/" in name:
                continue
            entries.append((name, "file" if isinstance(node, File) else "directory"))
        return entries

    def remove(self, path: str, *, recursive: bool) -> None:
        node = self.nodes.get(path)
        if node is None:
            raise FilesystemError("not_found")
        if isinstance(node, Directory):
            if not recursive:
                raise FilesystemError("is_directory")
            for candidate in [p for p in self.nodes if p.startswith(path.rstrip("/") + "/")]:
                del self.nodes[candidate]
        del self.nodes[path]

    def rename(self, source: str, destination: str) -> None:
        if source not in self.nodes:
            raise FilesystemError("not_found")
        if self.is_dir(destination):
            destination = posixpath.join(destination, posixpath.basename(source))
        moved = {
            candidate: node
            for candidate, node in self.nodes.items()
            if candidate == source or candidate.startswith(source.rstrip("/") + "/")
        }
        for candidate in moved:
            del self.nodes[candidate]
        self.mkdir(posixpath.dirname(destination), parents=True, exist_ok=True)
        for candidate, node in moved.items():
            self.nodes[destination + candidate[len(source) :]] = node

    def size(self) -> int:
        return sum(len(node.data) for node in self.nodes.values() if isinstance(node, File))

    def materialize(self, root: Path) -> None:
        """Write the filesystem under a host directory, for host programs."""
        for path, node in sorted(self.nodes.items()):
            target = root / path.lstrip("/")
            if isinstance(node, Directory):
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(node.data)
                os.chmod(target, node.mode)

    def absorb(self, root: Path) -> None:
        """Replace the contents with what a host program left under ``root``."""
        nodes: dict[str, Node] = {"/": Directory()}
        for directory, dirnames, filenames in os.walk(root):
            relative = Path(directory).relative_to(root).as_posix()
            base = "/" if relative == "." else "/" + relative
            for dirname in dirnames:
                nodes[posixpath.join(base, dirname)] = Directory()
            for filename in filenames:
                host = Path(directory) / filename
                nodes[posixpath.join(base, filename)] = File(
                    host.read_bytes(), stat.S_IMODE(host.stat().st_mode)
                )
        self.nodes = nodes

    @staticmethod
    def discard(root: Path) -> None:
        shutil.rmtree(root, ignore_errors=True)


__all__ = ["DEFAULT_CWD", "Directory", "FakeFilesystem", "File", "FilesystemError"]
