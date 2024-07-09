from __future__ import annotations

from pathlib import Path
import os


class PathRef:
    ref: str | Path | os.DirEntry

    @property
    def path(self) -> Path:
        return self.ref if isinstance(self.ref, Path) else Path(
            self.ref.path if isinstance(self.ref, os.DirEntry) else self.ref
        )

    @property
    def pathlike(self) -> os.PathLike:
        return self.ref.path if isinstance(self.ref, os.DirEntry) else self.ref

    @property
    def path_or_entry(self) -> Path | os.DirEntry:
        return Path(self.ref) if isinstance(self.ref, str) else self.ref

    def __init__(self, ref: str | Path | os.DirEntry = Path.cwd()):
        self.ref = ref

    def __str__(self) -> str:
        return self.ref if isinstance(self.ref, str) else (
            self.ref.path if isinstance(self.ref, os.DirEntry)
            else str(self.ref)
        )

    def __repr__(self) -> str:
        return f"PathRef(ref={self.ref!r})"

    def __eq__(self, rhs: PathRef) -> bool:
        return str(self) == str(rhs)

    def hash(self) -> int:
        return hash(str(self))

    def exists(self) -> bool:
        return True if isinstance(self.ref, os.DirEntry) \
            else self.path.exists()
