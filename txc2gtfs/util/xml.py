from __future__ import annotations

from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from lxml import etree

NS = {"txc": "http://www.transxchange.org.uk/"}


@overload
def get_text(base: etree.Element, path: str, *, default: str) -> str: ...


@overload
def get_text(base: etree.Element, path: str) -> str: ...


def get_text(base: etree.Element, path: str, **kwargs: str) -> str:
    text = base.findtext(path, kwargs.get("default"), NS)
    assert text, f"Could not find {path} in {base}"
    return text
