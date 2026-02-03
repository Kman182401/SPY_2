from __future__ import annotations

from pathlib import Path
import pkgutil


__path__ = pkgutil.extend_path(__path__, __name__)

_SRC_PACKAGE = Path(__file__).resolve().parent.parent / "src" / "spy2"
if _SRC_PACKAGE.is_dir():
    __path__.append(str(_SRC_PACKAGE))
