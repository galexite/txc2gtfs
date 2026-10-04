from __future__ import annotations

import dataclasses
from datetime import datetime
from enum import StrEnum

from lxml import etree


class Modification(StrEnum):
    NEW = "new"
    DELETE = "delete"
    REVISE = "revise"
    DELTA = "delta"
    ARCHIVE = "archive"


@dataclasses.dataclass(slots=True, frozen=True)
class Metadata:
    filename: str
    creation_date: datetime
    modification: Modification
    modification_date: datetime | None
    revision_num: int

    dataset_id: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", f"{self.filename}#{self.revision_num}")

    @staticmethod
    def from_xml_element(elem: etree.Element) -> Metadata:
        filename = elem.get("FileName")
        assert filename is not None
        creation_date = elem.get("CreationDateTime")
        assert creation_date is not None
        creation_date = datetime.fromisoformat(creation_date)
        modification = elem.get("Modification")
        assert modification is not None
        modification = Modification[modification]
        modification_date = elem.get("ModificationDateTime")
        if modification_date is not None:
            modification_date = datetime.fromisoformat(modification_date)
        revision_num = elem.get("RevisionNumber")
        assert revision_num is not None
        revision_num = int(revision_num)

        return Metadata(
            filename=filename,
            creation_date=creation_date,
            modification=modification,
            modification_date=modification_date,
            revision_num=revision_num,
        )
