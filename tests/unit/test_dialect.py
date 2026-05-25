"""Spike: verify ``CambrianSpark`` parses Iceberg's ``ADD PARTITION FIELD``.

PR #2 gates the architecture by checking that subclassing
``sqlglot.dialects.spark.Spark`` is a clean way to support Iceberg-specific
Spark DDL. See the PR body for the full assessment; these tests are the
machine-checkable half.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

from cambrian.sql.ast import AddPartitionField
from cambrian.sql.dialect import CambrianSpark


def _alter_actions(sql: str) -> list[exp.Expr]:
    """Parse one ALTER statement and return its ``actions`` list."""
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    assert len(tree) == 1, f"expected exactly one statement, got {len(tree)}"
    stmt = tree[0]
    assert isinstance(stmt, exp.Alter), f"expected Alter, got {type(stmt).__name__}"
    actions = stmt.args.get("actions") or []
    assert actions, "expected at least one alter action"
    return actions


def test_add_partition_field_with_bucket_transform() -> None:
    actions = _alter_actions("ALTER TABLE t ADD PARTITION FIELD bucket(16, x)")
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, AddPartitionField)

    # The transform call is preserved structurally so M5's dispatch layer
    # can route it to PyIceberg without re-parsing the source text.
    transform = action.args.get("transform")
    assert isinstance(transform, exp.Func)
    # ``Anonymous`` (unknown function name) stores the name under ``this``
    # as a plain string, with positional args under ``expressions``.
    assert transform.name.lower() == "bucket"
    transform_args = transform.args.get("expressions") or []
    assert len(transform_args) == 2
    assert isinstance(transform_args[0], exp.Literal)
    assert transform_args[0].name == "16"
    column_arg = transform_args[1]
    assert isinstance(column_arg, exp.Column)
    assert column_arg.name == "x"

    # ``this`` carries the source column reference (extracted from the
    # transform args) so consumers don't have to know per-transform arity.
    this = action.args.get("this")
    assert isinstance(this, exp.Column)
    assert this.name == "x"


def test_add_partition_field_with_bare_column() -> None:
    actions = _alter_actions("ALTER TABLE t ADD PARTITION FIELD x")
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, AddPartitionField)
    assert action.args.get("transform") is None

    this = action.args.get("this")
    # _parse_field returns an Identifier for a bare token; that's fine —
    # downstream code can wrap it in a Column if it needs a uniform shape.
    assert isinstance(this, exp.Identifier | exp.Column)
    assert this.name == "x"


def test_stock_add_column_still_parses() -> None:
    """Regression: subclassing must not break the parent's other ADD forms."""
    actions = _alter_actions("ALTER TABLE t ADD COLUMN c INT")
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, exp.ColumnDef)
    assert action.name == "c"
    assert isinstance(action.args.get("kind"), exp.DataType)
    assert action.args["kind"].this == exp.DataType.Type.INT


def test_dialect_can_be_passed_as_instance() -> None:
    """``sqlglot.parse(..., dialect=Cls)`` and ``dialect=Cls()`` both work."""
    sql = "ALTER TABLE t ADD PARTITION FIELD x"
    via_class = sqlglot.parse(sql, dialect=CambrianSpark)
    via_instance = sqlglot.parse(sql, dialect=CambrianSpark())
    # Compare structurally — instances are different objects, so we compare
    # the SQL re-rendering of the action node instead.
    via_class_action = via_class[0].args["actions"][0]
    via_instance_action = via_instance[0].args["actions"][0]
    assert type(via_class_action) is type(via_instance_action) is AddPartitionField
    assert via_class_action.args["this"].name == via_instance_action.args["this"].name
