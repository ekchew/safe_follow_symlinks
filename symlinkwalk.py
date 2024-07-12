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
_g_debug: bool = False


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


class _InLinkElem(str):
    #   Path elements are normally just plain str instances, but this subclass
    #   flags them as being part of a symlink's stored path that is being
    #   resolved.
    pass


class _InLinkPath(PathRef):
    #   Likewise, a tentative new PathRef that is being evaluated can be
    #   flagged as part of a symlink's resolution using this subclass.
    #   We need to keep track of this so that we can distinguish between
    #   broken symlinks and ordinary missing paths.
    pass


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

    #   Private attributes:
    #       _symlinks: a stack of symlinks encountered during recursion
    #       _elem_stack: a stack of path elements
    #           These are elements yet to be added to the path we are trying to
    #           construct. For example, if we have the path '/foo' so far with
    #           the stack looking like ['baz', 'bar'], the path should
    #           eventually grow out to '/foo/bar/baz' with the stack emptying
    #           to [] in the simplest case.
    #       _yield_fn: one of either _yield_path() or _yield_contents()
    #           The yield function is the only method that can ultimately
    #           yield output from the iter_dir() and iter_tree() generators.
    #           _yield_path simply yields the one PathRef it gets, and it used
    #           by resolve_path() and iter_dir(). _yield_contents() yields its
    #           immediate PathRef also, but then if it is a directory, it will
    #           recurse into it. As such, it is used by iter_tree().
    _symlinks: list[PathRef] = field(repr=_g_debug)
    _elem_stack: list[str] = field(repr=_g_debug)
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

        #   We need to allocate a temporary SymlinkWalk to walk through the
        #   path elements given to us.
        slw = cls()

        #   If the input path is absolute, we can begin a new path with just
        #   the root element of it and place the remaining elements on the
        #   stack.
        if pathRef.path.is_absolute():
            newPath = PathRef(pathRef.path.parts[0])

            #   Given that _elem_stack is a stack, the remaining elements need
            #   to populate it in reverse order. This odd-looking slice should
            #   reverse everything past the zeroth element.
            slw._elem_stack.extend(pathRef.path.parts[:0:-1])

        #   With a relative path, the new path should begin with the current
        #   working directory, which PathRef() should give us by default. Then
        #   all of the remaining elements should be pushed onto the stack.
        else:
            newPath = PathRef()
            slw._elem_stack.extend(reversed(pathRef.path.parts))

        #   The _scan() generator should hopefully yield a single PathRef built
        #   by extending newPath with each path element from the stack, with
        #   any symlinks resolved along the way.
        try:
            return next(slw._scan(newPath))

        #   If nothing emerges from _scan(), that means something went wrong,
        #   but it should placed what it has in bad_paths instead.
        except StopIteration:
            badPath = next(iter(slw.bad_paths))

            #   In the strict resolution case, we need to raise an exception at
            #   this point.
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

            #   Otherwise, we simply return the bad path and leave it up to the
            #   caller to determine how to proceed with it.
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

        #   Call resolve_path() to make sure we have a fully resolved path if
        #   necessary.
        if resolved:
            target = pathRef
        else:
            target = self.resolve_path(pathRef, expand_user=expand_user)
            if not target.exists():
                self.bad_paths.add(target)
                return

        #   Make sure we are dealing with a directory. Otherwise, there are
        #   no contents to iterate over.
        if pathRef.path_or_entry.is_dir():

            #   Select the non-recursive yield function.
            self._yield_fn = self._yield_path

            #   SymlinkWalk uses os.scandir() to generate directory listings,
            #   as it tends to be very fast compared os.listdir() or
            #   pathlib.Path.iterdir().
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

        #   In this case, we want to use the recursive _yield_contents() yield
        #   function.
        self._yield_fn = self._yield_contents
        yield from self._yield_contents(target)

    def reset(self):
        '''
        This method clears all of the output containers and internal buffers
        to release memory resources. It is automatically called as you exit the
        with block if you allocated the SymlinkWalk as a context manager.
        '''
        self.path_hits.clear()
        self.bad_paths.clear()
        self.skipped.clear()
        self._symlinks.clear()
        self._elem_stack.clear()

    def _scan(self, pathRef: PathRef) -> Iterator[PathRef]:
        #   _scan() is a recursive method which does most of the actual work.

        #   The first thing we want to screen if whether or not the path
        #   actually exists?
        if not pathRef.exists():

            #   If it does not, we want to return early without yielding it,
            #   but not before we add it to the bad_paths. Depending on whether
            #   the section of path we are constructing is within a symlink's
            #   stored path or not determines whether it should be reported as
            #   a BrokenLink or just a more general MissingPath.
            if isinstance(pathRef, _InLinkPath):
                self.bad_paths.add(BrokenLink(self._symlinks[-1].ref))
            else:
                self.bad_paths.add(MissingPath(
                    pathRef.path.joinpath(*reversed(self._elem_stack))
                ))

            #   We want to clear out any path elements left on the stack before
            #   returning early like this, lest some earlier recursion of
            #   _scan() tries to keep adding them.
            self._elem_stack.clear()
            return

        #   We need to make sure the path is normalized before we can run it
        #   against filters and what not. pathlib classes happily collapse path
        #   sequences like '//' or './' for you automatically, but '..' is left
        #   intact, so we need to deal with that.
        if pathRef.path_or_entry.name == '..':
            pathRef = PathRef(os.path.normpath(pathRef))

        #   Now we can run the path_filter to check if that path should be
        #   excluded?
        if not self.path_filter(pathRef):
            self.skipped.add(pathRef)
            self._elem_stack.clear()
            return

        #   Next, let's consider the yield_unique case. If we already have a
        #   hit on the current path, we want to increment the hit count and
        #   return early. Otherwise, we make a new entry with the count set to
        #   1 and continue.
        if self.yield_unique:
            try:
                self.path_hits[pathRef] += 1
            except KeyError:
                self.path_hits[pathRef] = 1
            else:
                self._elem_stack.clear()
                return

        symlink = pathRef.path_or_entry.is_symlink()
        try:
            if symlink:

                #   Whenever we encounter a symlink, we need to check if it is
                #   on the stack of symlinks we have already encountered?
                #   If so, we want to flag it as a recursive link not follow
                #   it again.
                if pathRef in self._symlinks:
                    self.bad_paths.add(RecursiveLink(pathRef.ref))
                    self._elem_stack.clear()
                    return

                #   Otherwise, this new one goes on the stack.
                self._symlinks.append(pathRef)

                #   Next, we need to read the stored path out of it.
                link = pathRef.path.readlink()

                #   If it is an absolute symlink, the pathRef should be reset
                #   to the root element and the remainder of it should be
                #   pushed onto the path element stack.
                if link.is_absolute():
                    pathRef = PathRef(link.parts[0])

                    #   Note that the elements are are adding now are flagged
                    #   as in-link elements. We need to record them as such so
                    #   that we can tell we are in a broken link should one of
                    #   them turn out to be missing.
                    self._elem_stack.extend(
                        map(_InLinkElem, link.parts[:0:-1])
                    )

                #   Otherwise, the path needs to be continued relative to the
                #   parent directory containing the symlink.
                else:
                    pathRef = PathRef(pathRef.path.parent)
                    self._elem_stack.extend(
                        map(_InLinkElem, reversed(link.parts))
                    )

            #   At this point, we need to check if there are any more elements
            #   waiting on the stack.
            if self._elem_stack:

                #   That being the case, we should pop off the next one and
                #   extend the current path with it to form a new subpath.
                #   In doing so, we need to keep track of whether the next path
                #   element--and therefore the subpath--are within a symlink's
                #   store path.
                part = self._elem_stack.pop()
                in_link = isinstance(part, _InLinkElem)
                subPath = _InLinkPath(pathRef.path/part) if in_link \
                    else PathRef(pathRef.path/part)

                #   This is where we call _scan() again recursively to handle
                #   the subpath.
                yield from self._scan(subPath)

            else:
                #   Once the path element stack is exhausted, we can finally
                #   yield the path we have built. Note that in the case of
                #   iter_tree(), this may not necessarily be the end of the
                #   recursion, as its yield function--_yield_contents()--may
                #   yet call _scan() again if pathRef points to a subdirectory.
                yield from self._yield_fn(pathRef)

        #   Before _scan() can return, we must make certain that if this
        #   recursion pushed a new symlink onto the stack, it is popped back
        #   off to balance it.
        finally:
            if symlink:
                self._symlinks.pop()

    def _yield_path(self, pathRef: PathRef) -> Iterator[PathRef]:
        #   The non-recursive _yield_fn used by resolve_path() and iter_dir().
        yield pathRef

    def _yield_contents(self, pathRef: PathRef) -> Iterator[PathRef]:
        #   The recursive _yield_fn used by iter_tree().
        yield pathRef
        if pathRef.path_or_entry.is_dir():
            with os.scandir(pathRef) as sd:
                for entry in sd:
                    yield from self._scan(PathRef(entry))


# ==== Command Line Implementation ============================================


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

    #   Here, we implement a path_filter for SymlinkWalk that looks for any
    #   matches to an exclude pattern to reject the path. path_filter() is
    #   enclosed with the _get_path_filter() method so that it can always
    #   access its patterns argument.
    def path_filter(pathRef: PathRef) -> bool:
        s = str(pathRef)
        for pattern in patterns:
            if fnmatch.fnmatchcase(s, pattern):
                return False
        return True

    return path_filter if patterns else SymlinkWalk.allow_all_paths


def _print_existing_path(pr: PathRef):
    #   A function to print existing paths to stdout.
    code = 'd' if pr.path_or_entry.is_dir() else 'f'
    print(code, pr)


def _print_bad_path(pr: PathRef):
    #   A function to print broken/recursive symlinks or missing paths.
    if pr.is_broken_link():
        print('b', pr)
    elif pr.is_recursive_link():
        print('r', pr)
    else:
        print('m', pr)


if __name__ == '__main__':

    #   Parse the command line and pull the target paths out of it in
    #   PathRef form.
    args = _parse_command_line()
    targets = [PathRef(p) for p in args.targets] if args.targets \
        else [PathRef()]

    try:
        if args.resolve == 'path':
            for target in targets:
                pr = SymlinkWalk.resolve_path(target)
                if pr.is_bad_path():
                    _print_bad_path(pr)
                else:
                    _print_existing_path(pr)
        else:
            #   For the iterative 'list' and 'tree' options, the path filter
            #   is relevant.
            path_filter = _get_path_filter(args.exclude)

            #   Create a new SymlinkWalk instance we will use to scan all of
            #   the targets.
            slw = SymlinkWalk(
                path_filter=path_filter, yield_unique=args.unique_paths
            )

            for target in targets:
                if args.resolve == 'list':
                    for pr in slw.iter_dir(target):
                        _print_existing_path(pr)
                else:
                    for pr in slw.iter_tree(target):
                        _print_existing_path(pr)

            #   Having completed the scan and printed all of the 'f' and 'd'
            #   entries, it is time to finish up with the 'x', 'u#', 'b', 'r',
            #   and 'm' ones.
            for pr in sorted(slw.skipped):
                print('x', pr)
            if args.unique_paths:
                #   Only print the paths with more than 1 hit.
                for pr, n in sorted(
                    (pr, n) for pr, n in slw.path_hits.items() if n > 1
                ):
                    print(f'u{n}', pr)
            for pr in sorted(slw.bad_paths):
                _print_bad_path(pr)

    except Exception as ex:
        print('ERROR:', ex, file=sys.stderr)
        if _g_debug:
            print_exc()
        sys.exit(1)
