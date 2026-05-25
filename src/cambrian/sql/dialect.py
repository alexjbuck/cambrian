"""``CambrianSpark`` — a sqlglot Spark dialect extended for Iceberg constructs.

PR #2 introduces the dialect with a single override — parsing
``ALTER TABLE t ADD PARTITION FIELD [transform]`` into
:class:`cambrian.sql.ast.AddPartitionField`. The remaining Iceberg DDL
extensions (``DROP PARTITION FIELD``, ``REPLACE PARTITION FIELD``,
``WRITE ORDERED BY``) land in M5 via the same extension pattern.

Why subclass instead of regex-preprocessing the SQL:

sqlglot's ALTER dispatch (``Parser.ALTER_PARSERS``) routes the ``ADD`` token
to :meth:`Parser._parse_alter_table_add`. That base method has a closure
(``_parse_add_alteration``) that handles known ADD shapes — including
``ADD PARTITION (...)`` for partition values — and returns ``None`` when
it sees an unfamiliar token sequence. ``PARTITION FIELD`` falls through
because ``FIELD`` isn't ``L_PAREN``. We intercept at the dialect's
``_parse_alter_table_add`` level: if the next two tokens are
``PARTITION FIELD`` we emit our node, otherwise we delegate to the parent
method untouched. No copy-paste of parent logic, no token rewinding.
"""

from __future__ import annotations

from sqlglot import expressions as exp
from sqlglot.dialects.spark import Spark
from sqlglot.parsers.spark import SparkParser

from cambrian.sql.ast import AddPartitionField


def _column_arg(func: exp.Func) -> exp.Expr | None:
    """Return the first Column/Identifier argument of an Iceberg transform.

    Iceberg's transform calls (``bucket(N, col)``, ``truncate(N, col)``,
    ``years(col)``, ``identity(col)``, ...) always reference exactly one
    source column. This helper extracts it so callers don't need to know
    the per-transform arity. Returns ``None`` if no column argument is
    present (shouldn't happen for valid Iceberg DDL, but we degrade
    gracefully).
    """
    for arg in func.args.get("expressions") or []:
        if isinstance(arg, exp.Column | exp.Identifier):
            return arg
    return None


class CambrianSpark(Spark):
    """Spark dialect with Iceberg DDL extensions.

    Use via ``sqlglot.parse(sql, dialect=CambrianSpark)`` or
    ``sqlglot.parse(sql, dialect=CambrianSpark())``.
    """

    class Parser(SparkParser):
        def _parse_alter_table_add(self) -> list[exp.Expr]:
            # By the time this method runs, the outer ALTER dispatch
            # (Parser._parse_alter via ALTER_PARSERS) has already consumed
            # the ADD token. So we peek for the next two tokens directly.
            #
            # ``_match_text_seq`` advances on a full match and is a no-op
            # (rewinds via ``_retreat``) on partial match, so a miss leaves
            # the parser positioned exactly where the parent expects.
            if self._match_text_seq("PARTITION", "FIELD"):
                # _parse_field handles both bare identifiers (``x``) and
                # function calls (``bucket(16, x)``), returning an
                # ``exp.Column``/``exp.Identifier`` for the former and an
                # ``exp.Anonymous``/typed ``exp.Func`` for the latter.
                #
                # For the AST shape we want ``this`` to always carry the
                # partition column reference and ``transform`` to carry the
                # transform call (if any). For ``bucket(16, x)`` that means
                # extracting the column-arg out of the function.
                parsed = self._parse_field(any_token=True)
                if isinstance(parsed, exp.Func):
                    column = _column_arg(parsed)
                    node = AddPartitionField(this=column, transform=parsed)
                else:
                    node = AddPartitionField(this=parsed)
                return [self.expression(node)]

            return super()._parse_alter_table_add()
