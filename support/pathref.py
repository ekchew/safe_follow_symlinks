'''
This module defines the PathRef class, along with a number of subclasses that
may be instantiated by symlinkwalk methods under certain circumstances.

symlinkwalk uses PathRef exclusively when dealing with paths.
'''

from __future__ import annotations

from pathlib import Path, PurePath
import os


class PathRef:
    '''
    File system paths can appear in a number of different forms in the Standard
    Library, and this wrapper class attempts to unify the more common among
    them.

    PathRef is a path-like class (i.e. it satisfies os.PathLike), and can
    therefore be used with most path-oriented APIs in the os module.

    PathRefs can also be sorted and used as keys in sets or dicts.

    Instance Attributes:
        ref: a path in bytes, str, pathlib.PurePath, or os.DirEntry form
            Defaults to Path.cwd().
    '''
    __slots__ = ['ref']

    @property
    def path(self) -> PurePath:
        '''
        Returns: ref attribute as a pathlib.Path
            (This assumes you are working with paths in your native
            envirnoment, which would almost certainly be the case when working
            together with symlinkwalk. If you are, for some reason, managing
            Windows paths from a POSIX system or vice versa, you will probably
            want to avoid this property.)
        '''
        return self.ref if isinstance(self.ref, Path) else Path(
            self.ref.path if isinstance(self.ref, os.DirEntry) else self.ref
        )

    @property
    def path_or_entry(self) -> Path | os.DirEntry:
        '''
        Returns: ref attribute as a pathlib.Path or os.DirEntry
            Since Path (though not necessarily PurePath) and DirEntry have a
            lot of APIs in common, it can be useful to access a path in either
            of those two forms.
        '''
        ready = isinstance(self.ref, Path) or isinstance(self.ref, os.DirEntry)
        return self.ref if ready else Path(self.ref)

    def __init__(
        self, ref: bytes | str | Path | os.DirEntry | None = None
    ):
        self.ref = Path.cwd() if ref is None else ref

    def __str__(self) -> str:
        s: str
        if isinstance(self.ref, PurePath):
            s = str(self.ref)
        else:
            sb: str | bytes
            if isinstance(self.ref, os.DirEntry):
                sb = self.ref.path
            else:
                sb = self.ref
            s = sb if isinstance(sb, str) else os.fsdecode(sb)
        return s

    def __repr__(self) -> str:
        return f'{type(self).__name__}(ref={self.ref!r})'

    def __eq__(self, rhs: PathRef) -> bool:
        return str(self) == str(rhs)

    def __lt__(self, rhs: PathRef) -> bool:
        return str(self).lower() < str(rhs).lower()

    def __hash__(self) -> int:
        return hash(str(self))

    def __fspath__(self) -> str:
        return str(self)

    def exists(self) -> bool:
        '''
        Returns: True if a file system object is thought to exist at the path
            If ref is an os.DirEntry, this is assumed to be so since
            os.scandir() would not have yielded it otherwise. In other cases,
            os.path.exists() is typically called, though this behaviour may be
            altered by a subclass.
        '''
        return True if isinstance(self.ref, os.DirEntry) \
            else os.path.exists(self.ref)

    def is_broken_link(self) -> bool:
        '''
        Returns: True if the path is to a confirmed broken symlink
            A False return value does not necessarily mean it is NOT a broken
            symlink, however. (The True value is returned by a subclass.)
        '''
        return False

    def is_recursive_link(self) -> bool:
        '''
        Returns: True if the path is a symlink found to recurse on itself
            A False return value does not necessarily mean it is NOT recursive,
            however. (The True value is returned by a subclass.)
        '''
        return False

    def is_bad_link(self) -> bool:
        '''
        This method simply combines calls to is_broken_link() and
        is_recursive_link(), returning True if either returns True.
        '''
        return self.is_broken_link() or self.is_recursive_link()

    def is_bad_path(self) -> bool:
        '''
        This method returns True if the path is known to be a bad link or not
        exist.
        '''
        #   Note: we need not call is_broken_link() in this case since
        #   BrokenLink subclasses MissingPath. Checking if self.exists()
        #   returns False should suffice.
        return self.is_recursive_link() or not self.exists()


class MissingPath(PathRef):
    '''
    A subclass of PathRef in which the exists() method always returns False.
    symlinkwalk.SymlinkWalk.resolve_path() may return one of these.
    '''
    def exists(self) -> bool:
        return False


class BrokenLink(MissingPath):
    '''
    A subclass of MissingPath in which is_broken_link() returns True.

    (Technically, the symlink file itself does exist in this case, but since
    the convention in terms of Standard Library APIs seems to be that exists()
    methods should return False for broken symlinks, BrokenLink adheres to this
    principle.)
    '''
    def is_broken_link(self) -> bool:
        return True


class RecursiveLink(PathRef):
    '''
    A subclass of PathRef in which the is_recursive_link() method always
    returns True.
    '''
    def is_recursive_link(self) -> bool:
        return True
