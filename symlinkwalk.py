#!/usr/bin/env python3


'''
This is both an importable module and a command-line script. To use it in the
latter form, enter `path/to/symlinkwalk.py --help` for more info.
'''


from support.pathref import PathRef

from argparse import ArgumentParser
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from traceback import print_exc
import fnmatch
import os
import sys


#   Makes private SymlinkWalk attributes appear in repr() and prints a stack
#   crawl when an exception is caught in command line mode.
_g_debug: bool = True


@dataclass(slots=True)
class _PathElem:
    part: str
    in_link: bool = False


@dataclass(init=False)
class SymlinkWalk:
    '''
    This class offers a number of methods for resolving paths and walking
    directories safely while following symlinks. It manages any state needed by
    the recursive algorithms and reports problem paths through public
    attributes.
    '''
    path_filter: Callable[[PathRef], bool]
    yield_unique: bool

    path_hits: dict[PathRef, int]
    recursed: set[PathRef]
    missing: set[PathRef]
    skipped: set[PathRef]

    _symlinks: list[PathRef] = field(repr=_g_debug)
    _elem_stack: list[_PathElem] = field(repr=_g_debug)
    _yield_fn: Callable[[PathRef], Iterator[PathRef]] = field(repr=_g_debug)

    def __init__(
        self, path_filter: Callable[[PathRef], bool] | None = None,
        yield_unique: bool = False
    ):
        self.path_filter = path_filter if path_filter else self.allow_all_paths
        self.yield_unique = yield_unique
        self.path_hits = {}
        self.missing = set()
        self.recursed = set()
        self.skipped = set()
        self._symlinks = []
        self._elem_stack = []
        self._yield_fn = self._yield_path

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.reset()

    @staticmethod
    def allow_all_paths(pathRef: PathRef) -> bool:
        return True

    @classmethod
    def resolve_path(cls, pathRef: PathRef) -> tuple[PathRef, bool]:
        slw = cls()
        if pathRef.path.is_absolute():
            newPath = PathRef(pathRef.path.parts[0])
            slw._elem_stack[:] = map(_PathElem, pathRef.path.parts[:0:-1])
        else:
            newPath = PathRef()
            slw._elem_stack.extend(
                map(_PathElem, reversed(pathRef.path.parts))
            )
        try:
            return next(slw._scan(newPath)), True
        except StopIteration:
            return next(iter(slw.missing)), False

    def iter_dir(
        self, pathRef: PathRef, resolved: bool = False
    ) -> Iterator[PathRef]:
        if resolved:
            target = pathRef
        else:
            target, found = self.resolve_path(pathRef)
            if not found:
                return
        self._yield_fn = self._yield_path
        yield from self._yield_contents(target)

    def iter_tree(
        self, pathRef: PathRef, resolved: bool = False
    ) -> Iterator[PathRef]:
        if resolved:
            target = pathRef
        else:
            target, found = self.resolve_path(pathRef)
            if not found:
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
                    self._elem_stack.extend(
                        _PathElem(part, in_link=True)
                        for part in link.parts[:0:-1]
                    )
                else:
                    pathRef = PathRef(pathRef.path.parent)
                    self._elem_stack.extend(
                        _PathElem(part, in_link=True)
                        for part in reversed(link.parts)
                    )

            if pathRef.exists():
                if self._elem_stack:
                    pathRef = PathRef(pathRef.path/self._elem_stack.pop().part)
                    yield from self._scan(pathRef)
                else:
                    yield from self._yield_fn(pathRef)
            elif self._elem_stack and self._elem_stack[-1].in_link:
                self.missing.add(self._symlinks[-1])
            else:
                self.missing.add(pathRef)
        finally:
            if symlink:
                self._symlinks.pop()

    def _yield_path(self, pathRef: PathRef) -> Iterator[PathRef]:
        try:
            self.path_hits[pathRef] += 1
        except KeyError:
            self.path_hits[pathRef] = 1
            yield pathRef
        else:
            if not self.yield_unique:
                yield pathRef

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        yield from self._yield_path(pathRef)
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))

    def reset(self):
        self.path_hits.clear()
        self.missing.clear()
        self.recursed.clear()
        self.skipped.clear()
        self._symlinks.clear()
        self._elem_stack.clear()
        self._yield_fn = self._yield_path


def _parse_command_line():
    ap = ArgumentParser(
        description='''
            This script lets you follow symlinks without having to worry about
            infinite recursion. You can use it to resolve a single path, list
            all items in a directory (with any symlink items fully resolved),
            or walk entire directory trees. In any case, each line printed to
            stdout is composed of a code, a space, and an absolute path. The
            codes are mostly only a single character. They include: 'f' = file,
            'd' = directory, 'm' = missing item, 'x' = excluded path, and 'u#'
            = unique path encountered # times (where # > 1). 'f' is essentially
            anything that is not a directory, and can include devices, etc. 'm'
            may signify a broken symlink, in which case the symlink's path
            (rather than what it points to) is printed.
            '''
    )
    ap.add_argument(
        "targets", metavar="TARGET", nargs="*",
        help='''
            You can specify one or more target files or directories. A file
            will have its absolute path resolved if possible. A directory will
            have its entire content tree listed, given the default operating
            mode (see --resolve for more options). If you specify no targets,
            the current working directory will be the target.
            '''
    )
    ap.add_argument(
        "-r", "--resolve", default="tree",
        help='''
            There are 3 resolve modes you can access with this script.
            'path' prints a single line per target containing a fully resolved
            path where possible. It should be marked either 'f' or 'd' if it
            exists. If not, it will be marked 'm' and the printed path will
            indicate how far it had been resolved before a missing element was
            encountered. 'list' mode lists the immediate member of a directory
            target. 'tree' mode walks the entire directory tree and prints
            everything it can find.'''
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
            Despite the fact that this script prevents symlink recursion, it is
            still possible for the same path to appear more than once in a
            listing. For example, some directory 'foo' may get listed once, and
            then later on, there is a symlink to 'foo'. You can use the
            --unique-paths option to prevent listing any path twice as an 'f'
            or 'd' record. You can still check all paths that appeared more
            than once by looking at the 'u#' lines.
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
                pr, found = SymlinkWalk.resolve_path(target)
                if found:
                    _print_path(pr)
                else:
                    print("m", pr)
            else:
                with SymlinkWalk(
                    path_filter=path_filter, yield_unique=args.unique_paths
                ) as slw:
                    if args.resolve == 'path':
                        pr, found = slw.resolve_path(target)
                        if found:
                            _print_path(pr)
                        else:
                            print("m", pr)
                    elif args.resolve == 'list':
                        for pr in slw.iter_dir(target):
                            _print_path(pr)
                    else:
                        for pr in slw.iter_tree(target):
                            _print_path(pr)
                    for pr in sorted(slw.skipped):
                        print("x", pr)
                    for pr, n in sorted(
                        (pr, n) for pr, n in slw.path_hits.items() if n > 1
                    ):
                        print(f"u{n}", pr)
                    for pr in sorted(slw.recursed):
                        print("r", pr)
                    for pr in sorted(slw.missing):
                        print("m", pr)
    except Exception as ex:
        print("ERROR:", ex, file=sys.stderr)
        if _g_debug:
            print_exc()
        sys.exit(1)
