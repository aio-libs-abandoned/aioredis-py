import sys

if sys.version_info >= (3, 8):
    from typing import Protocol, TypedDict
else:
    from typing_extensions import Protocol, TypedDict

__all__ = ("Protocol", "TypedDict")
