#!/usr/bin/env python3


'''
symlinkwalk is a module built around the SymlinkWalk class.

The symlinkwalk.py script can also be run as a command line tool.
For more help on this, consult the README.md file or enter
`path/to/symlinkwalk.py --help`.
'''


from support.pathref import PathRef, MissingPath, BrokenLink, RecursiveLink

from argparse import ArgumentParser
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from shlex import quote
from traceback import print_exc
import fnmatch
import os
import sys


#   Makes private SymlinkWalk attributes appear in repr() and prints a stack
#   crawl when an exception is caught in command line mode.
_g_debug: bool = True


class BrokenLinkError(FileNotFoundError):
    '''
    An exception that may be raised by `SymlinkWalk.resolve_path()` with
    `strict=True`.
    '''


class RecursiveLinkError(RuntimeError):
    '''
    An exception that may be raised by `SymlinkWalk.resolve_path()` with
    `strict=True`.
    '''


@dataclass(slots=True)
class _PathElem:
    part: str
    in_link: bool = False


@dataclass(init=False)
class SymlinkWalk:
    '''
    This class offers a number of methods for resolving paths and walking
    directories safely while following symlinks. It manages any state needed by
    the recursive algorithms and reports problem paths through output
    attributes.

    It can optionally be instantiated as context manager. The reason for this
    is that instances can carry a lot of state, and reset() is automatically
    called on exiting the context to clear the various container attributes.

    Once you have instantiated a SymlinkWalk, you can call one of the generator
    methods: iter_dir() or iter_tree().

    There is also resolve_path(), which does not require an instance since it
    is a class method. (It does allocate a temporary one internally.)

    Input attributes (those you may wish to supply at instantiation or later):
        path_filter: a callback to control which paths get scanned
            The filter should return `True` to accept the path or False to skip
            it. The filter is called on every file, directory, and even
            partially resolved symlink paths. You can save `iter_tree()` a lot
            of work by not drilling down into uninteresting directories.
        yield_unique: never yield the same path twice? (default=False)
            While the algorithm stops any attempt by a symlink to recurse back
            into itself, it does not go so far as to prevent the same path from
            being yielded multiple times, particularly by `iter_tree()`. For
            example, a symlink may point back to a directory that had already
            been scanned. This option would prevent such a double scan.

    Output attributes (providing info after calling iterative methods):
        path_hits: a dict counting unique path encounters
            This is only updated when `yield_unique=True`.
        bad_paths: set of all problematic paths
            These include missing paths, broken symlinks, and recursive
            symlinks. The
        skipped: a set of all paths skipped when path_filter() returned False
    '''
    path_filter: Callable[[PathRef], bool]
    yield_unique: bool

    path_hits: dict[PathRef, int]
    bad_paths: set[PathRef]
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
        self.bad_paths = set()
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
        '''
        The default path_filter returns True for every path.
        '''
        return True

    @classmethod
    def resolve_path(
        cls, pathRef: PathRef, expand_user: bool = True, strict: bool = False
    ) -> PathRef:
        '''
        This is a class method which resolves a single path and any symlinks it
        contains where possible.

        Args:
            pathRef: an absolute or relative path
            expand_user: expands '~' and '~user' sequences within paths
                Defaults to True.
            strict: raise exception on bad path? (defaults to False)
                This may be one of:
                    FileNotFoundError: path does not exist
                    BrokenLinkError: symlinked path does not exist
                        Note: BrokenLinkError is also a FileNotFoundError
                        subclass.
                    RecursiveLinkError: symlink found to be recursive

        Returns: PathRef derived from input PathRef
            Note that this may be a subclass of PathRef (MissingPath,
            BrokenLink, or RecursiveLink) if anything went wrong during path
            resolution. You can call methods like is_bad_link() or exists() to
            check if this is the case.

            In the case of a bad link, the path will be to the symlink
            itself rather than whatever it is pointing to.

            If it is an otherwise-nonexistent object, the path will be to where
            it should go, so that you can then complete the path with new
            file/directory elements if appropriate.
        '''
        if expand_user:
            pathRef = PathRef(pathRef.path.expanduser())
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
            return next(slw._scan(newPath))
        except StopIteration:
            badPath = next(iter(slw.bad_paths))
            if strict:
                pathStr = quote(str(badPath))
                if badPath.is_recursive_link():
                    raise RecursiveLinkError(
                        f"{pathStr} is a recursive symlink"
                    )
                elif badPath.is_broken_link():
                    raise BrokenLinkError(
                        f"{pathStr} is a broken symlink"
                    )
                else:
                    raise FileNotFoundError(
                        f"{pathStr} does not exist"
                    )
            return badPath

    def iter_dir(
        self, pathRef: PathRef, resolved: bool = False,
        expand_user: bool = True
    ) -> Iterator[PathRef]:
        '''
        This is a generator that yields a PathRef for each path in the input
        directory, provided the path is not:

            1. excluded by the path_filter
            2. bad (i.e. non-existent or a broken/recursive symlink)
            3. another hit on the same path in yield_unique mode

        Failing 1, the skipped attr is assigned the excluded path.
        Failing 2, the bad_path attr is assigned the bad path.
        Failing 3, the hit count for the path_hits entry is incremented.

        (Note: In yield_unique mode, all unique paths wind up in path_hits with
        at least a single hit. Otherwise, the dict is left unmodified.)

        It is important to note that SymlinkWalk's internal state is updated by
        this generator, but nothing gets reset beforehand. The idea is that you
        may well be calling it from a recursive function that is walking the
        directory tree manually rather than using iter_tree(). As such,
        tracking symlink recursion should go on between iter_dir() calls by
        default. If you want to start from a clean slate every time, either
        call reset() or allocate a new SymlinkWalk instance.

        Args:
            pathRef: absolute or relative path to the directory to list
            resolved: pathRef has already been resolved? (default=False)
                If you are implementing your own directory tree walk, you may
                want to set this to True when recursing into a subdirectory,
                since everything leading up to the directory should already be
                resolved at that point. It will save a lot of needless work.
            expand_user: expand '~' and '~user' elements within pathRef?
                this option will be ignored if resolved=True

        Yields: PathRef
            These should be "good" paths in that any problem ones should have
            been diverted to bad_paths, etc. Note that a recursive symlink must
            be followed once, however, before it can be discovered to be
            recursive.
        '''
        if resolved:
            target = pathRef
        else:
            target = self.resolve_path(pathRef, expand_user=expand_user)
            if not target.exists():
                self.bad_paths.add(target)
                return
        self._yield_fn = self._yield_path
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))

    def iter_tree(
        self, pathRef: PathRef, resolved: bool = False,
        expand_user: bool = True
    ) -> Iterator[PathRef]:
        '''
        iter_tree() works much like iter_dir() except that it recurses down
        into subdirectories. It also yields all directory paths it encounters,
        including the initial pathRef you supply.

        It follows a depth-first traversal order.
        '''
        if resolved:
            target = pathRef
        else:
            target = self.resolve_path(pathRef, expand_user=expand_user)
            if not target.exists():
                self.bad_paths.add(target)
                return
        self._yield_fn = self._yield_contents
        yield from self._yield_contents(target)

    def reset(self):
        self.path_hits.clear()
        self.bad_paths.clear()
        self.skipped.clear()
        self._symlinks.clear()
        self._elem_stack.clear()
        self._yield_fn = self._yield_path

    def _scan(self, pathRef: PathRef) -> Iterator[PathRef]:
        if pathRef.path_or_entry.name == '..':
            pathRef = PathRef(os.path.normpath(pathRef))

        if not self.path_filter(pathRef):
            self.skipped.add(pathRef)
            self._elem_stack.clear()
            return

        if self.yield_unique:
            try:
                self.path_hits[pathRef] += 1
            except KeyError:
                self.path_hits[pathRef] = 1
            else:
                self._elem_stack.clear()
                return

        symlink: bool = False
        try:
            if pathRef.path_or_entry.is_symlink():
                if pathRef in self._symlinks:
                    self.bad_paths.add(RecursiveLink(pathRef.ref))
                    self._elem_stack.clear()
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
            else:
                if self._elem_stack and self._elem_stack[-1].in_link:
                    self.bad_paths.add(BrokenLink(self._symlinks[-1].ref))
                else:
                    self.bad_paths.add(MissingPath(
                        pathRef.path.joinpath(*reversed(self._elem_stack))
                    ))
                self._elem_stack.clear()
        finally:
            if symlink:
                self._symlinks.pop()

    def _yield_path(self, pathRef: PathRef) -> Iterator[PathRef]:
        yield pathRef

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        yield pathRef
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))


def _parse_command_line():
    ap = ArgumentParser(
        description='''
            This script lets you follow symlinks without having to worry about
            infinite recursion. You can use it to resolve a single path, list
            all items in a directory (with any symlink items fully resolved),
            or walk entire directory trees. It prints one line per path to
            stdout preceeded by a code such as 'f' for file or 'd' for
            directory. See the README.md file for a full list of possible codes
            and what they mean.
            '''
    )
    ap.add_argument(
        'targets', metavar='TARGET', nargs='*',
        help='''
            You can specify one or more target files or directories. If you
            specify no targets, the current working directory will be the
            default target.
            '''
    )
    ap.add_argument(
        '-r', '--resolve', default='tree',
        help='''
            There are 3 resolve modes you can access with this script. 'path'
            prints a single line per target containing a fully resolved path
            where possible. 'list' mode lists the immediate members of a
            directory target. 'tree' mode walks the entire directory tree and
            prints everything it can find.'''
    )
    ap.add_argument(
        '-x', '--exclude', action='append',
        help='''
            A glob-style (a.k.a. fnmatch) pattern you may use to exclude
            exploring certain paths. Matched paths still appear, but with the
            prefix 'x'. You may supply more than one exclude pattern. (Note:
            the matching is case-sensitive.)
            '''
    )
    ap.add_argument(
        '-u', '--unique-paths', action='store_true',
        help='''
            Despite the fact that this script prevents symlink recursion, it is
            still possible for the same path to appear more than once in a
            listing. You can use the --unique-paths option to prevent listing
            any path twice as an 'f' or 'd' record. You can still check which
            appeared more than once by looking for 'u#' lines in the output.
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
    code = 'd' if pr.path_or_entry.is_dir() else 'f'
    print(code, pr)


if __name__ == '__main__':
    args = _parse_command_line()
    try:
        path_filter = _get_path_filter(args.exclude)
        slw = SymlinkWalk(
            path_filter=path_filter, yield_unique=args.unique_paths
        )
        targets = [PathRef(p) for p in args.targets] if args.targets \
            else [PathRef()]
        if args.resolve == 'path':
            for target in targets:
                pr = SymlinkWalk.resolve_path(target)
                if pr.is_broken_link():
                    print('b', pr)
                elif pr.is_recursive_link():
                    print('r', pr)
                elif pr.exists():
                    _print_path(pr)
                else:
                    print('m', pr)
        else:
            for target in targets:
                if args.resolve == 'path':
                    pr = SymlinkWalk.resolve_path(target)
                    if pr.exists():
                        _print_path(pr)
                else:
                    if args.resolve == 'list':
                        for pr in slw.iter_dir(target):
                            _print_path(pr)
                    else:
                        for pr in slw.iter_tree(target):
                            _print_path(pr)
                    for pr in sorted(slw.skipped):
                        print('x', pr)
            if args.unique_paths:
                for pr, n in sorted(
                    (pr, n) for pr, n in slw.path_hits.items() if n > 1
                ):
                    print(f'u{n}', pr)
            for pr in slw.skipped:
                print("x", pr)
            for pr in sorted(slw.bad_paths):
                if pr.is_broken_link():
                    print('b', pr)
                elif pr.is_recursive_link():
                    print('r', pr)
                else:
                    print('m', pr)
    except Exception as ex:
        print('ERROR:', ex, file=sys.stderr)
        if _g_debug:
            print_exc()
        sys.exit(1)
