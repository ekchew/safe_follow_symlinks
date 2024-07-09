#!/usr/bin/env python3


if __name__ == '__main__':
    from pathref import PathRef
else:
    from .pathref import PathRef

from argparse import ArgumentParser
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from traceback import print_exc
import fnmatch
import os
import sys


_g_debug: bool = False


@dataclass(init=False)
class SymlinkWalk:
    path_filter: Callable[[PathRef], bool]
    unique_paths: set[PathRef] | None

    recursed: set[PathRef]
    missing: set[PathRef]
    skipped: set[PathRef]
    blocked: set[PathRef]

    _symlinks: list[PathRef] = field(repr=_g_debug)
    _part_stack: list = field(repr=_g_debug)
    _yield_fn: Callable[[PathRef], Iterator[PathRef]] = field(repr=_g_debug)

    def __init__(
        self, path_filter: Callable[[PathRef], bool] | None = None,
        unique_paths: set[PathRef] | None = None
    ):
        self.path_filter = path_filter if path_filter else self.allow_all_paths
        self.unique_paths = unique_paths
        self.missing = set()
        self.recursed = set()
        self.skipped = set()
        self.blocked = set()
        self._symlinks = []
        self._part_stack = []
        self._yield_fn = self._yield_path

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.reset()

    @staticmethod
    def allow_all_paths(pathRef: PathRef) -> bool:
        return True

    @classmethod
    def resolve_path(cls, pathRef: PathRef) -> PathRef | None:
        slw = cls()
        if pathRef.path.is_absolute():
            newPath = PathRef(pathRef.path.parts[0])
            slw._part_stack[:] = pathRef.path.parts[:0:-1]
        else:
            newPath = PathRef()
            slw._part_stack.extend(reversed(pathRef.path.parts))
        try:
            return next(slw._scan(newPath))
        except StopIteration:
            return None

    def iter_dir(
        self, pathRef: PathRef, resolved: bool = False
    ) -> Iterator[PathRef]:
        if resolved:
            target = pathRef
        elif (target := self.resolve_path(pathRef)) is None:
            return
        self._yield_fn = self._yield_path
        yield from self._yield_contents(target)

    def iter_tree(
        self, pathRef: PathRef, resolved: bool = False
    ) -> Iterator[PathRef]:
        if resolved:
            target = pathRef
        elif (target := self.resolve_path(pathRef)) is None:
            return
        self._yield_fn = self._yield_contents
        yield from self._yield_contents(target)

    def _scan(self, pathRef: PathRef) -> Iterator[PathRef]:
        if pathRef.path_or_entry.name == '..':
            pathRef = PathRef(os.path.normpath(pathRef))

        if not self.path_filter(pathRef):
            self.skipped.add(pathRef)
            return

        symlink: bool = False
        try:
            if pathRef.path_or_entry.is_symlink():
                if pathRef in self._symlinks:
                    self.recursed.add(pathRef)
                    return
                self._symlinks.append(pathRef)
                symlink = pathRef

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
            elif self._symlinks:
                self.missing.add(self._symlinks[-1])
            else:
                self.missing.add(pathRef)
        finally:
            if symlink:
                self._symlinks.pop()

    def _yield_path(self, pathRef: PathRef) -> Iterator[PathRef]:
        if self.unique_paths is None:
            yield pathRef
        elif pathRef in self.unique_paths:
            if self._symlinks and \
                    self.resolve_path(self._symlinks[-1]) == pathRef:
                self.blocked.add(self._symlinks[-1])
            else:
                self.blocked.add(pathRef)
        else:
            self.unique_paths.add(pathRef)
            yield pathRef

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        yield from self._yield_path(pathRef)
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))

    def reset(self):
        self.missing.clear()
        self.recursed.clear()
        self.skipped.clear()
        self.blocked.clear()
        self._symlinks.clear()
        self._part_stack.clear()
        self._yield_fn = self._yield_path


def _parse_command_line():
    ap = ArgumentParser(
        description='''
            A command line interface to the SymlinkWalk class which lets you
            safely follow symlinks without falling into infinite recursion
            situations. Each line printed to stdout has a format looking
            something like 'f /full/path'. The 'f' here signfies a file, as
            opposed to a 'd' for directory. You may also see the following
            codes: 'r' = recursive symlink, 'm' = missing path, 'b' = blocked
            path, and 'x' = excluded path. Recursive symlinks are followed once
            but never a second time. Missing paths are typically broken
            symlinks (in which case the symlink path itself is printed), though
            it's possible your primary target may also be missing? A blocked
            path is one that is flagged as already being printed in
            --unique-paths mode. If the path is the destination of a symlink,
            the symlink path will be printed instead of the resolved path.
            Finally, an excluded path is one which matched an --exclude
            pattern.
            '''
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
    ap.add_argument(
        "-x", "--exclude", action="append",
        help='''
            A glob-style (a.k.a. fnmatch) pattern you may use to exclude
            exploring certain paths. Matched paths still appear, but with the
            prefix 'x'. You may supply more than one exclude pattern. (Note:
            the matching is case-sensitive.)
            '''
    )
    ap.add_argument(
        "-u", "--unique-paths", action="store_true",
        help='''
            Despite the fact that this script prevents following the same
            symlink multiple times, it is still possible for the same path to
            appear more than once in a listing. For example, some directory
            'foo' may get listed once, and then later on, there is a symlink to
            'foo'. You can use the --unique-paths option to prevent listing any
            path twice.
            '''
    )

    return ap.parse_args()


def _get_path_filter(patterns: list[str]) -> Callable[[PathRef], bool]:

    def path_filter(pathRef: PathRef) -> bool:
        s = str(pathRef)
        for pattern in patterns:
            if fnmatch.fnmatchcase(s, pattern):
                return False
        return True

    return path_filter if patterns else SymlinkWalk.allow_all_paths


def _print_path(pr: PathRef):
    code = "d" if pr.path_or_entry.is_dir() else "f"
    print(code, pr)


if __name__ == "__main__":
    args = _parse_command_line()
    try:
        targets = [PathRef(p) for p in args.targets] if args.targets \
            else [PathRef()]
        for target in targets:
            path_filter = _get_path_filter(args.exclude)
            if args.resolve == 'path':
                pr = SymlinkWalk.resolve_path(target)
                if pr:
                    _print_path(pr)
                else:
                    print("m", pr)
            else:
                with SymlinkWalk(path_filter=path_filter) as slw:
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
                    for pr in sorted(slw.skipped):
                        print("x", pr)
                    for pr in sorted(slw.blocked):
                        print("b", pr)
                    for pr in sorted(slw.recursed):
                        print("r", pr)
                    for pr in sorted(slw.missing):
                        print("m", pr)
    except Exception as ex:
        print("ERROR:", ex, file=sys.stderr)
        if _g_debug:
            print_exc()
        sys.exit(1)
