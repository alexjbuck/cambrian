"""Tests for ``CambrianSpark`` — sqlglot dialect with Iceberg DDL extensions.

These tests cover both:

* Iceberg-specific constructs (custom AST nodes in :mod:`cambrian.sql.ast`).
* Regression: stock Spark DDL that we rely on still parses cleanly.

Snapshot-style equality is asserted on the rendered AST type + key args, not
on full string equality, so a future sqlglot bump doesn't break us on
incidental rendering changes.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

from cambrian.sql.ast import (
    AddPartitionField,
    DropPartitionField,
    ReplacePartitionField,
    UnsetTblProperties,
    WriteOrderedBy,
)
from cambrian.sql.dialect import CambrianSpark


def _alter_actions(sql: str) -> list[exp.Expr]:
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    assert len(tree) == 1, f"expected exactly one statement, got {len(tree)}"
    stmt = tree[0]
    assert isinstance(stmt, exp.Alter), f"expected Alter, got {type(stmt).__name__}"
    actions = stmt.args.get("actions") or []
    assert actions, "expected at least one alter action"
    return actions


def _single(sql: str) -> exp.Expression:
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    assert len(tree) == 1
    return tree[0]


# ---------------------------------------------------------------------------
# AddPartitionField (extends the spike tests in PR #2)
# ---------------------------------------------------------------------------


def test_add_partition_field_with_bucket_transform() -> None:
    actions = _alter_actions("ALTER TABLE t ADD PARTITION FIELD bucket(16, x)")
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, AddPartitionField)
    transform = action.args.get("transform")
    assert isinstance(transform, exp.Func)
    assert transform.name.lower() == "bucket"
    column_arg = (transform.args.get("expressions") or [])[1]
    assert isinstance(column_arg, exp.Column)
    assert column_arg.name == "x"
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
    assert isinstance(this, exp.Identifier | exp.Column)
    assert this.name == "x"


def test_add_partition_field_with_alias() -> None:
    actions = _alter_actions("ALTER TABLE t ADD PARTITION FIELD bucket(16, x) AS xb")
    action = actions[0]
    assert isinstance(action, AddPartitionField)
    alias = action.args.get("alias")
    assert alias is not None
    assert getattr(alias, "name", None) == "xb"


# ---------------------------------------------------------------------------
# DropPartitionField
# ---------------------------------------------------------------------------


def test_drop_partition_field_bare_column() -> None:
    actions = _alter_actions("ALTER TABLE t DROP PARTITION FIELD x")
    action = actions[0]
    assert isinstance(action, DropPartitionField)
    this = action.args.get("this")
    assert getattr(this, "name", None) == "x"


def test_drop_partition_field_transform() -> None:
    actions = _alter_actions("ALTER TABLE t DROP PARTITION FIELD bucket(16, x)")
    action = actions[0]
    assert isinstance(action, DropPartitionField)
    transform = action.args.get("transform")
    assert isinstance(transform, exp.Func)
    assert transform.name.lower() == "bucket"


# ---------------------------------------------------------------------------
# ReplacePartitionField
# ---------------------------------------------------------------------------


def test_replace_partition_field_basic() -> None:
    actions = _alter_actions("ALTER TABLE t REPLACE PARTITION FIELD x WITH bucket(8, y)")
    action = actions[0]
    assert isinstance(action, ReplacePartitionField)
    old = action.args.get("this")
    assert getattr(old, "name", None) == "x"
    transform = action.args.get("transform")
    assert isinstance(transform, exp.Func)
    assert transform.name.lower() == "bucket"


def test_replace_partition_field_with_alias() -> None:
    actions = _alter_actions("ALTER TABLE t REPLACE PARTITION FIELD x WITH bucket(8, y) AS yb")
    action = actions[0]
    assert isinstance(action, ReplacePartitionField)
    alias = action.args.get("alias")
    assert getattr(alias, "name", None) == "yb"


# ---------------------------------------------------------------------------
# WriteOrderedBy
# ---------------------------------------------------------------------------


def test_write_ordered_by_single_column() -> None:
    actions = _alter_actions("ALTER TABLE t WRITE ORDERED BY (a)")
    action = actions[0]
    assert isinstance(action, WriteOrderedBy)
    cols = action.args.get("expressions") or []
    assert len(cols) == 1
    # _parse_ordered wraps bare columns in exp.Ordered with desc=False default.
    assert isinstance(cols[0], exp.Ordered)


def test_write_ordered_by_with_direction() -> None:
    actions = _alter_actions("ALTER TABLE t WRITE ORDERED BY (a ASC, b DESC)")
    action = actions[0]
    assert isinstance(action, WriteOrderedBy)
    cols = action.args.get("expressions") or []
    assert len(cols) == 2
    assert cols[0].args.get("desc") in (False, None)
    assert cols[1].args.get("desc") is True


# ---------------------------------------------------------------------------
# Stock Spark forms (regression coverage)
# ---------------------------------------------------------------------------


def test_stock_add_column_still_parses() -> None:
    """Spike regression: subclassing must not break ADD COLUMN."""
    actions = _alter_actions("ALTER TABLE t ADD COLUMN c INT")
    action = actions[0]
    assert isinstance(action, exp.ColumnDef)
    assert action.name == "c"


def test_add_columns_plural_parses() -> None:
    actions = _alter_actions("ALTER TABLE t ADD COLUMNS (a INT, b STRING)")
    # ADD COLUMNS (a, b) parses to a single Schema action wrapping ColumnDefs.
    assert len(actions) == 1
    schema = actions[0]
    assert isinstance(schema, exp.Schema)
    cols = schema.args.get("expressions") or []
    assert [c.name for c in cols] == ["a", "b"]


def test_drop_column_singular_parses() -> None:
    """Iceberg-Spark accepts ``DROP COLUMN c`` (singular)."""
    actions = _alter_actions("ALTER TABLE t DROP COLUMN c")
    action = actions[0]
    assert isinstance(action, exp.Drop)
    assert (action.args.get("kind") or "").upper() == "COLUMN"
    inner = action.args.get("this")
    assert getattr(inner, "name", None) == "c"


def test_drop_columns_plural_parses() -> None:
    actions = _alter_actions("ALTER TABLE t DROP COLUMNS (a, b)")
    action = actions[0]
    assert isinstance(action, exp.Drop)
    assert (action.args.get("kind") or "").upper() == "COLUMNS"


def test_rename_column_parses() -> None:
    actions = _alter_actions("ALTER TABLE t RENAME COLUMN a TO b")
    action = actions[0]
    assert isinstance(action, exp.RenameColumn)
    assert action.args["this"].name == "a"
    assert action.args["to"].name == "b"


def test_alter_column_type_parses() -> None:
    actions = _alter_actions("ALTER TABLE t ALTER COLUMN a TYPE BIGINT")
    action = actions[0]
    assert isinstance(action, exp.AlterColumn)
    assert action.args["this"].name == "a"
    dtype = action.args.get("dtype")
    assert isinstance(dtype, exp.DataType)
    assert dtype.this == exp.DataType.Type.BIGINT


def test_set_tblproperties_parses() -> None:
    actions = _alter_actions('ALTER TABLE t SET TBLPROPERTIES ("k" = "v")')
    action = actions[0]
    assert isinstance(action, exp.AlterSet)


def test_unset_tblproperties_parses() -> None:
    actions = _alter_actions('ALTER TABLE t UNSET TBLPROPERTIES ("k")')
    action = actions[0]
    assert isinstance(action, UnsetTblProperties)
    keys = action.args.get("expressions") or []
    assert len(keys) == 1


def test_unset_tblproperties_multiple_keys() -> None:
    actions = _alter_actions('ALTER TABLE t UNSET TBLPROPERTIES ("a", "b", "c")')
    action = actions[0]
    assert isinstance(action, UnsetTblProperties)
    keys = action.args.get("expressions") or []
    assert len(keys) == 3


def test_create_namespace_parses() -> None:
    node = _single("CREATE NAMESPACE foo")
    assert isinstance(node, exp.Create)
    assert (node.args.get("kind") or "").upper() == "NAMESPACE"


def test_create_namespace_if_not_exists_parses() -> None:
    node = _single("CREATE NAMESPACE IF NOT EXISTS foo")
    assert isinstance(node, exp.Create)
    assert (node.args.get("kind") or "").upper() == "NAMESPACE"
    assert node.args.get("exists") is True


def test_drop_namespace_parses() -> None:
    node = _single("DROP NAMESPACE foo")
    assert isinstance(node, exp.Drop)
    assert (node.args.get("kind") or "").upper() == "NAMESPACE"


def test_drop_namespace_if_exists_parses() -> None:
    node = _single("DROP NAMESPACE IF EXISTS foo")
    assert isinstance(node, exp.Drop)
    assert node.args.get("exists") is True


def test_create_table_parses() -> None:
    node = _single("CREATE TABLE foo.t (id INT) USING iceberg")
    assert isinstance(node, exp.Create)
    assert (node.args.get("kind") or "").upper() == "TABLE"


def test_create_table_if_not_exists_parses() -> None:
    node = _single("CREATE TABLE IF NOT EXISTS foo.t (id INT) USING iceberg")
    assert isinstance(node, exp.Create)
    assert node.args.get("exists") is True


def test_drop_table_parses() -> None:
    node = _single("DROP TABLE foo.t")
    assert isinstance(node, exp.Drop)
    assert (node.args.get("kind") or "").upper() == "TABLE"


def test_insert_values_parses() -> None:
    node = _single("INSERT INTO foo.t VALUES (1, 2), (3, 4)")
    assert isinstance(node, exp.Insert)
    expression = node.args.get("expression")
    assert isinstance(expression, exp.Values)


def test_insert_select_parses_but_inner_is_select() -> None:
    """``INSERT ... SELECT`` parses as Insert but inner is Select; dispatch rejects."""
    node = _single("INSERT INTO foo.t SELECT * FROM bar")
    assert isinstance(node, exp.Insert)
    expression = node.args.get("expression")
    assert isinstance(expression, exp.Select)


# ---------------------------------------------------------------------------
# Multi-statement parsing
# ---------------------------------------------------------------------------


def test_multiple_statements_parse_in_order() -> None:
    sql = """
    CREATE NAMESPACE foo;
    CREATE TABLE foo.t (id INT) USING iceberg;
    INSERT INTO foo.t VALUES (1);
    """
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    types = [type(t).__name__ for t in tree if t is not None]
    assert types == ["Create", "Create", "Insert"]


def test_dialect_can_be_passed_as_instance() -> None:
    sql = "ALTER TABLE t ADD PARTITION FIELD x"
    via_class = sqlglot.parse(sql, dialect=CambrianSpark)
    via_instance = sqlglot.parse(sql, dialect=CambrianSpark())
    via_class_action = via_class[0].args["actions"][0]
    via_instance_action = via_instance[0].args["actions"][0]
    assert type(via_class_action) is type(via_instance_action) is AddPartitionField
