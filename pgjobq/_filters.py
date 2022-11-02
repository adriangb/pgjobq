from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Sequence, Union
from uuid import UUID

ScalarValue = Union[str, int, float, bool]
Params = List[Any]


SCALAR_TYPE_CONVERSION = {
    int: "bigint",
    bool: "boolean",
    float: "float",
    str: "text",
}


class BaseClause:
    def get_value(self, params: List[Any]) -> str:
        raise NotImplementedError

    def __or__(self, __o: object) -> BaseClause:
        if not isinstance(__o, BaseClause):
            return NotImplemented
        return ClausePair(self, __o, "OR")

    def __and__(self, __o: object) -> BaseClause:
        if not isinstance(__o, BaseClause):
            return NotImplemented
        return ClausePair(self, __o, "AND")


@dataclass(frozen=True)
class ClausePair(BaseClause):
    left: BaseClause
    right: BaseClause
    op: str

    def get_value(self, params: List[Any]) -> str:
        left_val = self.left.get_value(params)
        right_val = self.right.get_value(params)
        return f"({left_val} {self.op} {right_val})"


@dataclass(frozen=True)
class AttributeComparisonClause(BaseClause):
    attribute: str
    value: ScalarValue
    operator: str

    def get_value(self, params: List[Any]) -> str:
        params.append(self.value)
        cast_to = SCALAR_TYPE_CONVERSION[type(self.value)]
        return f"(pgjobq.jobs.attributes ? '{self.attribute}' AND CAST(pgjobq.jobs.attributes->>'{self.attribute}' AS {cast_to}) {self.operator} ${len(params)})"


@dataclass(frozen=True)
class AttributeNullClause(BaseClause):
    attribute: str
    not_: bool

    def get_value(self, params: List[Any]) -> str:
        flip = "NOT " if self.not_ else ""
        return f"(pgjobq.jobs.attributes ? '{self.attribute}' AND pgjobq.jobs.attributes->>'{self.attribute}' IS {flip}NULL)"


@dataclass(frozen=True)
class AttributeLikeClause(BaseClause):
    attribute: str
    value: str
    not_: bool

    def get_value(self, params: List[Any]) -> str:
        flip = "NOT " if self.not_ else ""
        params.append(self.value)
        return f"(pgjobq.jobs.attributes ? '{self.attribute}' AND pgjobq.jobs.attributes->>'{self.attribute}' {flip}LIKE ${len(params)})"


@dataclass(frozen=True)
class AttributeExistsClause(BaseClause):
    attribute: str
    not_: bool

    def get_value(self, params: List[Any]) -> str:
        flip = "NOT" if self.not_ else ""
        return f"{flip}( pgjobq.jobs.attributes ? '{self.attribute}')"


@dataclass
class Attribute:
    attribute_name: str

    def _predicate_clause(
        self, value: ScalarValue, operator: str
    ) -> AttributeComparisonClause:
        return AttributeComparisonClause(
            attribute=self.attribute_name, value=value, operator=operator
        )

    def _null_clause(self, not_: bool) -> BaseClause:
        return AttributeNullClause(self.attribute_name, not_)

    def _like_clause(self, value: str, not_: bool) -> BaseClause:
        return AttributeLikeClause(self.attribute_name, value, not_)

    def eq(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, "=")

    def ne(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, "<>")

    def lt(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, "<")

    def gt(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, ">")

    def le(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, "<=")

    def ge(self, value: ScalarValue) -> BaseClause:
        return AttributeComparisonClause(self.attribute_name, value, ">=")

    def is_null(self) -> BaseClause:
        return AttributeNullClause(self.attribute_name, False)

    def is_not_null(self) -> BaseClause:
        return AttributeNullClause(self.attribute_name, True)

    def is_like(self, value: str) -> BaseClause:
        return AttributeLikeClause(self.attribute_name, value, False)

    def is_not_like(self, value: str) -> BaseClause:
        return AttributeLikeClause(self.attribute_name, value, True)

    def exists(self) -> BaseClause:
        return AttributeExistsClause(self.attribute_name, False)

    def does_not_exist(self) -> BaseClause:
        return AttributeExistsClause(self.attribute_name, True)


@dataclass(frozen=True)
class JobIdIn(BaseClause):
    ids: Sequence[UUID]

    def get_value(self, params: List[Any]) -> str:
        params.append(list(self.ids))
        return f"pgjobq.jobs.id = any(${len(params)}::uuid[])"
