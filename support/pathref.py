from __future__ import annotations

from pathlib import Path, PurePath
import os


class PathRef:
    '''
    File system paths can appear in a number of different forms in the Standard
    Library, and this wrapper class attempts to unify the more common among
    them.

    PathRef is a path-like class (i.e. satisfies os.PathLike), and can
    therefore be used with most path-oriented APIs in the os module.

    PathRefs can also be sorted and used as keys in sets or dicts.

    Attributes:
        ref: a path in bytes, str, pathlib.PurePath, or os.DirEntry form
            Defaults to Path.cwd().
        encoding: encoding used to convert bytes form of ref into a str
            Defaults to 'utf-8'.
    '''
    __slots__ = ['ref', 'encoding']

    @property
    def path(self) -> PurePath:
        '''
        Returns: ref attribute as a pathlib.PurePath
            If ref is something other than a PurePath already, the returned
            value will be a pathlib.Path. This presumes you are managing paths
            in the native environment. If you want to manage Windows paths from
            a POSIX system or vice versa, you may want to avoid this property.
        '''
        return self.ref if isinstance(self.ref, PurePath) else Path(
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
        self, ref: bytes | str | Path | os.DirEntry | None = None,
        encoding: str = "utf-8"
    ):
        self.ref = Path.cwd() if ref is None else ref
        self.encoding = encoding

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
            s = sb if isinstance(sb, str) else sb.decode(self.encoding)
        return s

    def __repr__(self) -> str:
        return f"PathRef(ref={self.ref!r})"

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
            os.path.exists() is called.
        '''
        return True if isinstance(self.ref, os.DirEntry) \
            else os.path.exists(self.ref)
