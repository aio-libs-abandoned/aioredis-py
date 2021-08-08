import sys

if sys.version_info >= (3, 8):
    from typing import (
        Protocol as Protocol,
        TypedDict as TypedDict,
    )
else:
    from typing_extensions import (
        Protocol as Protocol,
        TypedDict as TypedDict,
    )
