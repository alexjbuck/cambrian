"""Custom ``sqlglot.exp.Expression`` subclasses for Iceberg Spark extensions.

This module is the home of AST nodes that don't exist in stock sqlglot but
are needed to faithfully represent Iceberg's Spark SQL extensions
(see https://iceberg.apache.org/docs/latest/spark-ddl/ for the dialect).

PR #2 introduces one node — ``AddPartitionField`` — as a spike to validate
the sqlglot-extension architecture. Remaining Iceberg DDL nodes
(``DropPartitionField``, ``ReplacePartitionField``, ``WriteOrderedBy``)
land in M5.
"""

from __future__ import annotations

import typing as t

from sqlglot import expressions as exp


class AddPartitionField(exp.Expression):
    """``ALTER TABLE t ADD PARTITION FIELD [transform]``.

    Represents the Iceberg-specific ``ADD PARTITION FIELD`` clause inside an
    ``ALTER TABLE`` statement. The clause has two valid shapes:

    * ``ADD PARTITION FIELD x`` — partition by the bare column ``x``.
      ``this`` is the column reference; ``transform`` is ``None``.
    * ``ADD PARTITION FIELD bucket(16, x)`` — partition by a transform call.
      ``transform`` is the ``Func``/``Anonymous`` call; ``this`` is unset
      (caller may still populate ``this`` with the inner column for
      convenience, but it isn't required).

    The split between ``this`` and ``transform`` mirrors how stock sqlglot
    represents ``AddPartition`` (``this`` carries the partition spec) while
    preserving the structured transform call for the dispatch layer (M5),
    which needs to map the transform name + args to a PyIceberg
    ``UpdateSpec`` operation.
    """

    arg_types: t.ClassVar[dict[str, bool]] = {"this": True, "transform": False}
