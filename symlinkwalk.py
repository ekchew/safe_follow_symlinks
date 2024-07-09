from .pathref import PathRef

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
import os


_g_debug: bool = False


def _yield_path(pathRef: PathRef) -> Iterator[PathRef]:
    yield pathRef


@dataclass(init=False)
class SymlinkWalk:
    missing: list[PathRef]
    repeated: list[PathRef]

    _seen_links: set[PathRef] = field(repr=_g_debug)
    _part_stack: list = field(repr=_g_debug)
    _resolve_fn: Callable[[PathRef], Iterator[PathRef]] = field(repr=_g_debug)

    def __init__(self):
        self.missing = []
        self.repeated = []
        self._seen_links = set()
        self._part_stack = []
        self._resolve_fn = _yield_path

    def resolve_path(self, pathRef: PathRef) -> PathRef | None:
        if pathRef.path.is_absolute():
            newPath = PathRef(pathRef.path.parts[0])
            self._part_stack = pathRef.path.parts[:0:-1]
        else:
            newPath = PathRef()
            self._part_stack.extend(reversed(pathRef.path.parts))
        try:
            return next(self._scan(newPath))
        except StopIteration:
            return None

    def iter_dir(self, pathRef: PathRef) -> Iterator[PathRef]:
        self._resolve_fn = self._yield_path
        yield from self._yield_contents(pathRef)

    def iter_tree(self, pathRef: PathRef) -> Iterator[PathRef]:
        self._resolve_fn = self._yield_contents
        yield from self._yield_contents(pathRef)

    def _scan(self, pathRef: PathRef) -> Iterator[PathRef]:
        if pathRef.path_or_entry.name == '..':
            pathRef = PathRef(os.path.normpath(pathRef.pathlike))

        if pathRef.path_or_entry.is_symlink():
            if pathRef in self._seen_links:
                self.repeated.append(pathRef)
                return
            self._seen_links.add(pathRef)

            link = pathRef.path.readlink()
            if link.is_absolute():
                pathRef = PathRef(link.parts[0])
                self._part_stack = link.parts[:0:-1]
            else:
                self._part_stack.extend(reversed(link.parts))

        if pathRef.exists():
            if self._part_stack:
                pathRef = PathRef(pathRef.path/self._part_stack.pop())
                yield from self._scan(pathRef)
            else:
                yield from self._resolve_fn(pathRef)
        else:
            self.missing.append(pathRef)

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef.pathlike) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))
        else:
            yield pathRef

    def reset(self):
        self.missing.clear()
        self.repeated.clear()
        self._seen_links.clear()
        self._part_stack.clear()
        self._resolve_fn = _yield_path
