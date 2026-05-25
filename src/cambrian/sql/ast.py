"""Custom ``sqlglot.exp.Expression`` subclasses for Iceberg Spark extensions.

This module is the home of AST nodes that don't exist in stock sqlglot but
are needed to faithfully represent Iceberg's Spark SQL extensions
(see https://iceberg.apache.org/docs/latest/spark-ddl/ for the dialect).

Custom nodes:

- :class:`AddPartitionField` — ``ALTER TABLE t ADD PARTITION FIELD [transform]``
- :class:`DropPartitionField` — ``ALTER TABLE t DROP PARTITION FIELD <name>``
- :class:`ReplacePartitionField` — ``ALTER TABLE t REPLACE PARTITION FIELD <name> WITH <transform>``
- :class:`WriteOrderedBy` — ``ALTER TABLE t WRITE ORDERED BY (<cols>)``

Namespace operations (``CREATE NAMESPACE``, ``DROP NAMESPACE``) are NOT new
node types — stock Spark already parses them into ``exp.Create`` /
``exp.Drop`` with ``kind="NAMESPACE"``. The dispatch layer handles the
namespace case by reading ``kind``.
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
      ``transform`` is the ``Func``/``Anonymous`` call; ``this`` is set to
      the inner column when the transform's argument shape lets us extract
      it (the common ``bucket(N, col)`` / ``truncate(N, col)`` case). For
      typed-Func transforms where sqlglot puts the column under ``Func.this``
      (e.g. ``Year(this=col)``), the dispatch layer pulls it from the
      transform node.

    Optional ``alias`` carries an explicit partition-field name when the SQL
    uses ``... AS <name>``.
    """

    # ``this`` is optional because for typed transforms like ``year(col)``
    # sqlglot's _parse_field returns a Func whose ``this`` is the column; we
    # leave AddPartitionField.this unset and dispatch reads it from
    # ``transform.this`` instead.
    arg_types: t.ClassVar[dict[str, bool]] = {
        "this": False,
        "transform": False,
        "alias": False,
    }


class DropPartitionField(exp.Expression):
    """``ALTER TABLE t DROP PARTITION FIELD <name>``.

    ``this`` is the column-or-transform reference whose partition field
    should be removed. The Iceberg surface accepts either the bare column
    name (when the partition was created with an identity transform under a
    name matching the column) or a transform call like
    ``DROP PARTITION FIELD bucket(16, x)``. We store the parsed reference
    verbatim and let dispatch translate to the right ``UpdateSpec`` call.
    """

    # See AddPartitionField — ``this`` is optional for typed-Func transforms.
    arg_types: t.ClassVar[dict[str, bool]] = {"this": False, "transform": False}


class ReplacePartitionField(exp.Expression):
    """``ALTER TABLE t REPLACE PARTITION FIELD <old> WITH <transform> [AS <name>]``.

    ``this`` is the existing partition field to be replaced; ``transform``
    is the new transform expression; ``alias`` is the new field name when
    supplied.
    """

    arg_types: t.ClassVar[dict[str, bool]] = {
        "this": True,
        "transform": False,
        "alias": False,
    }


class WriteOrderedBy(exp.Expression):
    """``ALTER TABLE t WRITE ORDERED BY (<col ASC|DESC NULLS FIRST|LAST>, ...)``.

    ``expressions`` is a list of :class:`sqlglot.exp.Ordered` nodes (or bare
    column references when no direction was specified — sqlglot's default
    behaviour). Dispatch (M5c) iterates the list and maps each ordered
    column to a PyIceberg ``UpdateSortOrder.asc/.desc`` call.
    """

    arg_types: t.ClassVar[dict[str, bool]] = {"expressions": True}


class UnsetTblProperties(exp.Expression):
    """``ALTER TABLE t UNSET TBLPROPERTIES (k1, k2, ...)``.

    Distinct AST node from ``exp.AlterSet`` because UNSET has different
    dispatch semantics (it calls ``Transaction.remove_properties`` rather
    than ``set_properties``). Stock Spark falls back to ``Command`` for
    UNSET TBLPROPERTIES, so the dialect must intercept and emit this node.

    ``expressions`` is a list of property keys (``exp.Identifier`` /
    ``exp.Literal`` / ``exp.Column``). Dispatch normalises each to its
    string form for the PyIceberg API.
    """

    arg_types: t.ClassVar[dict[str, bool]] = {"expressions": True}
