"""``CambrianSpark`` — a sqlglot Spark dialect extended for Iceberg constructs.

This dialect adds parsing for the Iceberg-Spark DDL extensions that stock
``sqlglot.dialects.spark.Spark`` doesn't recognise:

* ``ALTER TABLE t ADD PARTITION FIELD [transform [AS name]]``
* ``ALTER TABLE t DROP PARTITION FIELD <ref>``
* ``ALTER TABLE t REPLACE PARTITION FIELD <old> WITH <new> [AS name]``
* ``ALTER TABLE t WRITE ORDERED BY (<cols...>)``
* ``ALTER TABLE t DROP COLUMN <name>`` (singular — stock Spark only knows
  the ``DROP COLUMNS (a, b)`` plural form)
* ``ALTER TABLE t UNSET TBLPROPERTIES (<keys>)``

Extension pattern (per spike PR #2): override the minimum surface on
:class:`SparkParser` to detect the Iceberg-specific token sequences and
emit cambrian's custom AST nodes (:mod:`cambrian.sql.ast`); fall through to
``super()`` for everything else. The parent's parser is never rewritten or
forked — only intercepted.

``CREATE NAMESPACE`` / ``DROP NAMESPACE`` already parse in stock Spark
(into ``exp.Create``/``exp.Drop`` with ``kind="NAMESPACE"``). No override
needed; dispatch reads the ``kind`` to route.
"""

from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.dialects.spark import Spark
from sqlglot.parsers.spark import SparkParser

from cambrian.sql.ast import (
    AddPartitionField,
    DropPartitionField,
    ReplacePartitionField,
    UnsetTblProperties,
    WriteOrderedBy,
)

__all__ = ["CambrianSpark"]


def _column_arg(func: exp.Func) -> exp.Expr | None:
    """Return the first Column/Identifier argument of an Iceberg transform call.

    Iceberg's transform calls (``bucket(N, col)``, ``truncate(N, col)``,
    ``years(col)``, ``identity(col)``, ...) always reference exactly one
    source column. This helper extracts it so callers don't need to know
    the per-transform arity. Returns ``None`` if no column argument is
    present (shouldn't happen for valid Iceberg DDL, but degrades gracefully).
    """
    for arg in func.args.get("expressions") or []:
        if isinstance(arg, exp.Column | exp.Identifier):
            return arg
    return None


def _parse_partition_field_payload(
    parser: SparkParser,
) -> tuple[exp.Expr | None, exp.Func | None, exp.Expr | None]:
    """Parse ``[<transform> | <col>] [AS <name>]`` and return (this, transform, alias).

    Used by both ``ADD PARTITION FIELD`` and ``REPLACE PARTITION FIELD WITH``
    so the shared shape of the payload doesn't drift between them.
    """
    parsed = parser._parse_field(any_token=True)
    if isinstance(parsed, exp.Func):
        column = _column_arg(parsed)
        this: exp.Expr | None = column
        transform: exp.Func | None = parsed
    else:
        this = parsed
        transform = None
    alias: exp.Expr | None = None
    if parser._match_text_seq("AS"):
        alias = parser._parse_field(any_token=True)
    return this, transform, alias


class CambrianSpark(Spark):
    """Spark dialect with Iceberg DDL extensions.

    Use via ``sqlglot.parse(sql, dialect=CambrianSpark)`` or
    ``sqlglot.parse(sql, dialect=CambrianSpark())``.
    """

    class Parser(SparkParser):
        # Two ALTER_PARSERS entries don't exist in stock Spark: WRITE (for
        # ``WRITE ORDERED BY``) and REPLACE (for ``REPLACE PARTITION FIELD``).
        # We extend by copying the parent's dict and adding the new keys
        # so the parent class's mapping isn't mutated in-place.
        ALTER_PARSERS: t.ClassVar[dict[str, t.Callable[..., t.Any]]] = {
            **SparkParser.ALTER_PARSERS,
            "WRITE": lambda self: self._parse_alter_table_write(),
            "REPLACE": lambda self: self._parse_alter_table_replace(),
            # UNSET TBLPROPERTIES gets its own entry because stock Spark
            # only registers SET; without this UNSET falls to Command.
            "UNSET": lambda self: self._parse_alter_table_unset(),
        }

        def _parse_alter_table_add(self) -> list[exp.Expr]:
            # Intercept ``ADD PARTITION FIELD <payload>`` before the parent's
            # _parse_add_alteration closure (which only knows ``ADD PARTITION
            # (...)``). On a non-match we leave the parser position untouched
            # via ``_match_text_seq``'s built-in rewind and delegate.
            if self._match_text_seq("PARTITION", "FIELD"):
                this, transform, alias = _parse_partition_field_payload(self)
                node = AddPartitionField(this=this, transform=transform, alias=alias)
                return [self.expression(node)]
            return super()._parse_alter_table_add()

        def _parse_alter_table_drop(self) -> list[exp.Expr]:
            # Intercept ``DROP PARTITION FIELD <ref>`` before parent's csv
            # split. We pop into the same shape parent uses (a list of action
            # nodes) so the surrounding _parse_alter machinery is none the
            # wiser.
            if self._match_text_seq("PARTITION", "FIELD"):
                parsed = self._parse_field(any_token=True)
                if isinstance(parsed, exp.Func):
                    column = _column_arg(parsed)
                    node: exp.Expression = DropPartitionField(this=column, transform=parsed)
                else:
                    node = DropPartitionField(this=parsed)
                return [self.expression(node)]
            return super()._parse_alter_table_drop()

        def _parse_alter_drop_action(self) -> exp.Expr | None:
            # Stock Spark's drop_column only handles ``DROP COLUMNS (a, b)``
            # (plural with parens) — singular ``DROP COLUMN c`` falls through.
            # Iceberg-Spark supports the singular form, so we handle it here.
            if self._match_text_seq("DROP", "COLUMN"):
                column = self._parse_column()
                if column is not None:
                    return self.expression(exp.Drop(this=column, kind="COLUMN"))
            return super()._parse_alter_drop_action()

        def _parse_alter_table_write(self) -> list[exp.Expr]:
            # ``WRITE ORDERED BY (cols...)``. Only ORDERED BY is in scope for
            # v1 — ``WRITE DISTRIBUTED BY`` and friends raise back through the
            # parser's fall-back-to-Command path on purpose.
            if self._match_text_seq("ORDERED", "BY"):
                self._match_l_paren()
                items = self._parse_csv(self._parse_ordered)
                self._match_r_paren()
                # _parse_ordered returns exp.Ordered (with .this and .args["desc"])
                # — we keep them as-is so dispatch can read direction directly.
                node = WriteOrderedBy(expressions=items)
                return [self.expression(node)]
            # Anything else after WRITE is unsupported; fall through to None
            # so the surrounding alter parser bails out cleanly. Returning an
            # empty list keeps the Alter intact but with no actions, which the
            # dispatch layer rejects with a clear error.
            return []

        def _parse_alter_table_replace(self) -> list[exp.Expr]:
            # ``REPLACE PARTITION FIELD <old> WITH <new> [AS <name>]``.
            if self._match_text_seq("PARTITION", "FIELD"):
                old = self._parse_field(any_token=True)
                if not self._match_text_seq("WITH"):
                    # ``REPLACE PARTITION FIELD x`` with no WITH clause is
                    # malformed in Iceberg-Spark. Return empty so dispatch
                    # raises a clear error rather than emitting a half-built
                    # node.
                    return []
                this, transform, alias = _parse_partition_field_payload(self)
                # If WITH was followed by a bare column (no transform), keep
                # the column as this; the ReplacePartitionField node still
                # carries transform=None and dispatch handles both shapes.
                node = ReplacePartitionField(
                    this=old,
                    transform=transform if transform is not None else this,
                    alias=alias,
                )
                return [self.expression(node)]
            return []

        def _parse_alter_table_unset(self) -> list[exp.Expr]:
            # ``UNSET TBLPROPERTIES (k1, k2, ...)``. Stock Spark only registers
            # SET in ALTER_PARSERS, so UNSET TBLPROPERTIES falls through to
            # Command. We emit a custom ``UnsetTblProperties`` AST node that
            # dispatch routes to ``Transaction.remove_properties``.
            if self._match_text_seq("TBLPROPERTIES"):
                self._match_l_paren()
                keys = self._parse_csv(lambda: self._parse_field(any_token=True))
                self._match_r_paren()
                # Defensive: parse_csv may include None entries if the input
                # is malformed; filter so the dispatch layer always sees
                # well-formed key nodes.
                clean_keys = [k for k in keys if k is not None]
                return [self.expression(UnsetTblProperties(expressions=clean_keys))]
            return []
