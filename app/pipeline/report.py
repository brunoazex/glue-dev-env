from dataclasses import dataclass
from typing import Optional


@dataclass
class LoaderSection(object):
    date_ref: Optional[str] = None
    predicate: Optional[str] = None
    row_count: Optional[int] = None
    status: Optional[str] = "unknown"


@dataclass
class CleanupSection(object):
    raw_count: Optional[int] = None
    duplicated_count: Optional[int] = None
    de_duplicated_count: Optional[int] = None
    no_description: Optional[int] = None
    description_count: Optional[int] = None
    no_code_count: Optional[int] = None
    code_count: Optional[int] = None
    valid_rows: Optional[int] = None
    status: Optional[str] = "unknown"


@dataclass
class ProcessSection(object):
    raw_count: Optional[int] = None
    matched_count: Optional[int] = None
    not_matched_count: Optional[int] = None
    new_count: Optional[int] = None
    repeated_count: Optional[int] = None
    transformed_count: Optional[int] = None
    valid_rows: Optional[int] = None
    status: Optional[str] = "unknown"


@dataclass
class PersistSection(object):
    raw_count: Optional[int] = None
    valid_rows: Optional[int] = None
    status: Optional[str] = "unknown"


@dataclass
class Report(object):
    elapsed: Optional[float] = 0.0
    status: Optional[str] = "unknown"
    additional_info: Optional[str] = None
    load: Optional[LoaderSection] = LoaderSection()
    cleanup: Optional[CleanupSection] = CleanupSection()
    process: Optional[ProcessSection] = ProcessSection()
    persist: Optional[PersistSection] = PersistSection()
