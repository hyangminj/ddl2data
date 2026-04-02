from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DistSpec:
    kind: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class ColumnMeta:
    name: str
    type_name: str
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    max_length: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class UniqueConstraintMeta:
    columns: list[str]
    name: str | None = None


@dataclass
class CheckConstraintMeta:
    expression: str
    name: str | None = None


@dataclass
class TableMeta:
    name: str
    columns: list[ColumnMeta]
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    unique_constraints: list[UniqueConstraintMeta] = field(default_factory=list)
    check_constraints: list[CheckConstraintMeta] = field(default_factory=list)

    def column(self, name: str) -> ColumnMeta:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"Column not found: {name}")
