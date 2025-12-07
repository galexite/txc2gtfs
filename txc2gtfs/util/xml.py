from __future__ import annotations

from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from lxml import etree

NS = {"txc": "http://www.transxchange.org.uk/"}


@overload
def get_text[T](base: etree.Element, path: str, *, default: T) -> str | T: ...


@overload
def get_text(base: etree.Element, path: str) -> str: ...


def get_text[T](base: etree.Element, path: str, **kwargs: T) -> str | T:
    el = base.find(path, NS)
    if "default" in kwargs and el is None:
        return kwargs["default"]
    assert el is not None
    text = el.text
    if "default" in kwargs:
        return text or kwargs["default"]
    assert text
    return text
