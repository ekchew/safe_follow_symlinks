#!/usr/bin/env python3


if __name__ == '__main__':
    from pathref import PathRef
else:
    from .pathref import PathRef

from argparse import ArgumentParser
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
import os


_g_debug: bool = False


def _yield_path(pathRef: PathRef) -> Iterator[PathRef]:
    return iter((pathRef,))


@dataclass(init=False)
class SymlinkWalk:
    symlinks: set[PathRef]
    repeats: set[PathRef]
    missing: set[PathRef]

    _part_stack: list = field(repr=_g_debug)
    _yield_fn: Callable[[PathRef], Iterator[PathRef]] = field(repr=_g_debug)

    def __init__(self):
        self.missing = set()
        self.repeats = set()
        self.symlinks = set()
        self._part_stack = []
        self._yield_fn = _yield_path

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.reset()

    def resolve_path(self, pathRef: PathRef) -> PathRef | None:
        if pathRef.path.is_absolute():
            newPath = PathRef(pathRef.path.parts[0])
            self._part_stack[:] = pathRef.path.parts[:0:-1]
        else:
            newPath = PathRef()
            self._part_stack.extend(reversed(pathRef.path.parts))
        try:
            return next(self._scan(newPath))
        except StopIteration:
            return None

    def iter_dir(self, pathRef: PathRef) -> Iterator[PathRef]:
        self._yield_fn = _yield_path
        yield from self._yield_contents(pathRef)

    def iter_tree(self, pathRef: PathRef) -> Iterator[PathRef]:
        self._yield_fn = self._yield_contents
        yield from self._yield_contents(pathRef)

    def _scan(self, pathRef: PathRef) -> Iterator[PathRef]:
        if pathRef.path_or_entry.name == '..':
            print(str(pathRef))
            pathRef = PathRef(os.path.normpath(pathRef.pathlike))
            print("->", str(pathRef))

        if pathRef.path_or_entry.is_symlink():
            if pathRef in self.symlinks:
                self.repeats.add(pathRef)
                return
            self.symlinks.add(pathRef)

            link = pathRef.path.readlink()
            if link.is_absolute():
                pathRef = PathRef(link.parts[0])
                self._part_stack[:] = link.parts[:0:-1]
            else:
                pathRef = PathRef(pathRef.path.parent)
                self._part_stack.extend(reversed(link.parts))

        if pathRef.exists():
            if self._part_stack:
                pathRef = PathRef(pathRef.path/self._part_stack.pop())
                yield from self._scan(pathRef)
            else:
                yield from self._yield_fn(pathRef)
        else:
            self.missing.add(pathRef)

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        yield pathRef
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef.pathlike) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))

    def reset(self):
        self.missing.clear()
        self.repeats.clear()
        self.symlinks.clear()
        self._part_stack.clear()
        self._yield_fn = _yield_path


def _parse_command_line():
    ap = ArgumentParser(
        description='''
            A command line interface to the SymlinkWalk class which lets you
            safely follow symlinks without falling into infinite recursion
            situations. Each line printed to stdout has a format looking
            something like 'f /full/path'. The 'f' here signfies a file, as
            opposed to a 'd' for directory. At the end of the listing, you may
            also see some 's' (well-behaved symlinks), 'r' (repeated symlinks),
            and 'm' (missing items). A repeated symlink may indicate recursion
            or several parallel paths merging into the same place. In either
            case, it will not be followed a second time. Missing items may
            arise from broken symlinks, though a missing primary target may
            also be flagged with an 'm'.'''
    )
    ap.add_argument(
        "targets", metavar="TARGET", nargs="*",
        help='''
            You can specify one or more target files or directories. A file
            will have its absolute path resolved if possible. A directory will
            have its entire content tree listed, given the default operating
            mode (see --resolve for more options). If you specify no targets,
            the current working directory will the target.
            '''
    )
    ap.add_argument(
        "-r", "--resolve", default="tree",
        help='''
            For directory targets, you have 3 choices as to how they should be
            scanned. The 'path' option simply prints the directory path
            fully resolved. The 'list' option prints the directory and its
            immediate members. The 'tree' option (the default) prints the
            entire directory tree in depth-first order.'''
    )
    return ap.parse_args()


def _print_path(pr: PathRef):
    code = "d" if pr.path_or_entry.is_dir() else "f"
    print(code, pr)


if __name__ == "__main__":
    args = _parse_command_line()
    targets = [PathRef(p) for p in args.targets] if args.targets \
        else [PathRef()]
    for target in targets:
        with SymlinkWalk() as slw:
            if args.resolve == 'path':
                pr = slw.resolve_path(target)
                if pr:
                    _print_path(pr)
            elif args.resolve == 'list':
                for pr in slw.iter_dir(target):
                    _print_path(pr)
            else:
                for pr in slw.iter_tree(target):
                    _print_path(pr)
            for pr in sorted(slw.symlinks - slw.repeats):
                print("s", pr)
            for pr in sorted(slw.repeats):
                print("r", pr)
            for pr in sorted(slw.missing):
                print("m", pr)
