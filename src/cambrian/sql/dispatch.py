"""Translate parsed SQL statements into PyIceberg API calls.

Dispatch is a single ``match``/``isinstance`` over the parsed AST node and
delegates each construct to a private helper. Each helper takes the live
:class:`Catalog` and the statement node, performs the right PyIceberg call,
and returns a :class:`DispatchResult` carrying the touched tables and any
notes.

**Idempotent contract.** Every handler must tolerate the "already at the
desired state" condition for ADD-likes (``NamespaceAlreadyExistsError``,
``TableAlreadyExistsError``, ``Duplicate partition field`` from
``UpdateSpec.add_field``) and the "already absent" condition for DROP-likes
when ``IF EXISTS`` is given. **No rollback on failure.** When a handler
raises, the runner emits a partial-apply event and propagates — restoring
state is M6's job.

**Multi-column ALTERs.** ``ALTER TABLE t ADD COLUMNS (a, b, c)`` is split
into N sequential ``update_schema`` commits because the REST catalog spec
requires one schema change per commit. Each commit is a fresh transaction.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)
from sqlglot import expressions as exp

from cambrian.errors import DispatchError, UnsupportedStatementError
from cambrian.iceberg.affected import TableIdent, affected_tables
from cambrian.sql.ast import (
    AddPartitionField,
    DropPartitionField,
    ReplacePartitionField,
    UnsetTblProperties,
    WriteOrderedBy,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

__all__ = ["DispatchResult", "dispatch"]


@dataclass
class DispatchResult:
    """The outcome of dispatching a single statement.

    ``affected_tables`` is the (possibly-empty) list of tables the statement
    targeted. ``notes`` is free-form text the runner copies into the event
    log for human consumption.
    """

    affected_tables: list[TableIdent] = field(default_factory=list)
    notes: str = ""


def dispatch(catalog: Catalog, statement: exp.Expression) -> DispatchResult:
    """Translate *statement* into PyIceberg calls and execute them.

    See module docstring for the idempotent contract.

    Raises:
        UnsupportedStatementError: The construct isn't in cambrian's v1
            supported list (e.g. ``INSERT ... SELECT``, ``MERGE``, ``DELETE``).
        DispatchError: The construct is recognised but cambrian can't run
            it (schema coercion failure, etc.).
    """
    # CREATE NAMESPACE / DROP NAMESPACE — kind=NAMESPACE on the Create/Drop.
    if isinstance(statement, exp.Create) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return _dispatch_create_namespace(catalog, statement)
    if isinstance(statement, exp.Drop) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return _dispatch_drop_namespace(catalog, statement)

    if isinstance(statement, exp.Create):
        return _dispatch_create_table(catalog, statement)
    if isinstance(statement, exp.Drop):
        return _dispatch_drop_table(catalog, statement)

    if isinstance(statement, exp.Alter):
        return _dispatch_alter(catalog, statement)

    if isinstance(statement, exp.Insert):
        return _dispatch_insert(catalog, statement)

    # The parser sometimes leaves a bare ``Semicolon`` at the end of a script
    # whose final statement is just a trailing comment. Treat as no-op so the
    # runner doesn't have to special-case it.
    if isinstance(statement, exp.Semicolon):
        return DispatchResult(notes="(no-op: bare semicolon)")

    raise UnsupportedStatementError(
        statement_sql=statement.sql(),
        reason=(
            f"unrecognised statement type {type(statement).__name__}; "
            "see plan §2.2 for the v1 supported list"
        ),
    )


def _kind(node: exp.Expression) -> str:
    return (node.args.get("kind") or "").upper()


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


def _namespace_from_table_node(node: exp.Expression) -> str:
    """Render a namespace identifier from an ``exp.Table`` node.

    ``CREATE NAMESPACE a.b`` parses to ``Table(this=b, db=a)`` so we render
    the multi-part dotted form by walking ``catalog`` → ``db`` → ``this``.
    """
    if not isinstance(node, exp.Table):
        return str(node)
    parts: list[str] = []
    cat = node.args.get("catalog")
    db = node.args.get("db")
    if cat is not None:
        parts.append(cat.name)
    if db is not None:
        parts.append(db.name)
    parts.append(node.name)
    return ".".join(parts)


def _dispatch_create_namespace(catalog: Catalog, stmt: exp.Create) -> DispatchResult:
    inner = stmt.args.get("this")
    if inner is None:
        raise DispatchError(f"CREATE NAMESPACE missing namespace identifier: {stmt.sql()}")
    namespace = _namespace_from_table_node(inner)
    try:
        catalog.create_namespace(namespace)
        notes = f"created namespace {namespace}"
    except NamespaceAlreadyExistsError:
        # IF NOT EXISTS is implicit under the idempotent contract — the
        # absence of the explicit clause doesn't change semantics for us.
        notes = f"namespace {namespace} already exists"
    return DispatchResult(notes=notes)


def _dispatch_drop_namespace(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    inner = stmt.args.get("this")
    if inner is None:
        raise DispatchError(f"DROP NAMESPACE missing namespace identifier: {stmt.sql()}")
    namespace = _namespace_from_table_node(inner)
    if_exists = bool(stmt.args.get("exists"))
    try:
        catalog.drop_namespace(namespace)
        notes = f"dropped namespace {namespace}"
    except NoSuchNamespaceError:
        if not if_exists:
            raise
        notes = f"namespace {namespace} already absent (IF EXISTS)"
    return DispatchResult(notes=notes)


# ---------------------------------------------------------------------------
# CREATE/DROP TABLE
# ---------------------------------------------------------------------------


def _table_identifier(table_node: exp.Table) -> tuple[str, ...]:
    """Convert an ``exp.Table`` into a tuple identifier for PyIceberg."""
    parts: list[str] = []
    cat = table_node.args.get("catalog")
    db = table_node.args.get("db")
    if cat is not None:
        parts.append(cat.name)
    if db is not None:
        parts.append(db.name)
    parts.append(table_node.name)
    return tuple(parts)


def _dispatch_create_table(catalog: Catalog, stmt: exp.Create) -> DispatchResult:
    inner = stmt.args.get("this")
    if not isinstance(inner, exp.Schema):
        raise DispatchError(
            "CREATE TABLE without a column list isn't supported (cambrian needs the schema "
            "to translate to PyIceberg). Use ``CREATE TABLE t (cols...) USING iceberg``."
        )
    table_node = inner.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"CREATE TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    column_defs = inner.args.get("expressions") or []
    schema = _build_iceberg_schema(column_defs)
    if_not_exists = bool(stmt.args.get("exists"))

    try:
        catalog.create_table(identifier=identifier, schema=schema)
        notes = f"created table {'.'.join(identifier)}"
    except TableAlreadyExistsError:
        if not if_not_exists:
            # Idempotent contract is implicit; even without IF NOT EXISTS we
            # tolerate "already exists" so re-applies of current.sql are
            # safe. The notes string makes the no-op visible in the log.
            notes = f"table {'.'.join(identifier)} already exists (idempotent re-apply)"
        else:
            notes = f"table {'.'.join(identifier)} already exists (IF NOT EXISTS)"
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=notes,
    )


def _dispatch_drop_table(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    inner = stmt.args.get("this")
    if not isinstance(inner, exp.Table):
        raise DispatchError(f"DROP TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(inner)
    if_exists = bool(stmt.args.get("exists"))
    try:
        catalog.drop_table(identifier)
        notes = f"dropped table {'.'.join(identifier)}"
    except NoSuchTableError:
        if not if_exists:
            # Same idempotent reasoning as create_table — tolerate the
            # "already absent" case even without IF EXISTS.
            notes = f"table {'.'.join(identifier)} already absent (idempotent re-apply)"
        else:
            notes = f"table {'.'.join(identifier)} already absent (IF EXISTS)"
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=notes,
    )


# ---------------------------------------------------------------------------
# ALTER TABLE
# ---------------------------------------------------------------------------


def _dispatch_alter(catalog: Catalog, stmt: exp.Alter) -> DispatchResult:
    table_node = stmt.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"ALTER missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    table = catalog.load_table(identifier)
    actions = stmt.args.get("actions") or []
    note_chunks: list[str] = []

    for action in actions:
        note_chunks.append(_dispatch_alter_action(catalog, table, identifier, action))
        # Reload the table after each action so subsequent actions see the
        # committed state. PyIceberg caches metadata aggressively and stale
        # state breaks the multi-action ALTER case (e.g. ADD then ALTER COLUMN
        # in one statement).
        table = catalog.load_table(identifier)

    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes="; ".join(c for c in note_chunks if c),
    )


def _dispatch_alter_action(
    catalog: Catalog, table: Table, identifier: tuple[str, ...], action: exp.Expression
) -> str:
    """Route a single ALTER action to its handler. Returns a notes string."""
    # ADD COLUMN (singular) — sqlglot emits a ColumnDef directly under actions.
    if isinstance(action, exp.ColumnDef):
        return _add_column(table, action)

    # ADD COLUMNS (a, b, c) — wrapped in a Schema; split into N commits.
    if isinstance(action, exp.Schema):
        col_defs = action.args.get("expressions") or []
        notes_parts: list[str] = []
        for col_def in col_defs:
            if not isinstance(col_def, exp.ColumnDef):
                raise DispatchError(
                    f"unexpected element inside ADD COLUMNS list: {type(col_def).__name__}"
                )
            notes_parts.append(_add_column(table, col_def))
            # Reload between additions; REST catalogs commit-per-update.
            table = catalog.load_table(identifier)
        return ", ".join(notes_parts)

    if isinstance(action, exp.Drop):
        return _drop_column(table, action)

    if isinstance(action, exp.RenameColumn):
        return _rename_column(table, action)

    if isinstance(action, exp.AlterColumn):
        return _alter_column(table, action)

    if isinstance(action, exp.AlterSet):
        return _set_tblproperties(table, action)

    if isinstance(action, UnsetTblProperties):
        return _unset_tblproperties(table, action)

    if isinstance(action, AddPartitionField):
        return _add_partition_field(table, action)
    if isinstance(action, DropPartitionField):
        return _drop_partition_field(table, action)
    if isinstance(action, ReplacePartitionField):
        return _replace_partition_field(catalog, table, identifier, action)
    if isinstance(action, WriteOrderedBy):
        return _write_ordered_by(table, action)

    raise UnsupportedStatementError(
        statement_sql=action.sql(),
        reason=(
            f"unsupported ALTER action {type(action).__name__}; see plan §2.2 "
            "for the v1 supported list"
        ),
    )


def _add_column(table: Table, col_def: exp.ColumnDef) -> str:
    name = col_def.name
    dtype = _iceberg_type_from_sqlglot(col_def.args.get("kind"))
    # ALTER TABLE ... ADD COLUMN is always nullable in Spark/Iceberg unless
    # the user gives a NOT NULL constraint — we err on the safe side
    # (required=False) so re-applies don't fight a NULL existing row.
    try:
        with table.update_schema() as us:
            us.add_column(name, dtype, required=False)
    except ValueError as err:
        # PyIceberg raises a ValueError on duplicate add. Idempotent contract.
        if "already exists" in str(err).lower() or "duplicate" in str(err).lower():
            return f"add column {name}: already exists (idempotent)"
        raise
    return f"add column {name}"


def _drop_column(table: Table, action: exp.Drop) -> str:
    inner = action.args.get("this")
    if inner is None:
        raise DispatchError(f"DROP COLUMN missing target: {action.sql()}")
    # The action's payload is either an exp.Column (singular form) or an
    # exp.Schema (plural with parens).
    kind = (action.args.get("kind") or "").upper()
    if kind == "COLUMNS" and isinstance(inner, exp.Schema):
        notes: list[str] = []
        for col in inner.args.get("expressions") or []:
            name = col.name
            try:
                with table.update_schema() as us:
                    us.delete_column(name)
                notes.append(f"drop column {name}")
            except ValueError as err:
                if "not found" in str(err).lower():
                    notes.append(f"drop column {name}: already absent (idempotent)")
                else:
                    raise
        return ", ".join(notes)
    # Singular form: ``DROP COLUMN c``.
    name = inner.name
    try:
        with table.update_schema() as us:
            us.delete_column(name)
    except ValueError as err:
        if "not found" in str(err).lower() or "does not exist" in str(err).lower():
            return f"drop column {name}: already absent (idempotent)"
        raise
    return f"drop column {name}"


def _rename_column(table: Table, action: exp.RenameColumn) -> str:
    old = action.args["this"].name
    new = action.args["to"].name
    try:
        with table.update_schema() as us:
            us.rename_column(old, new)
    except ValueError as err:
        # If old name is already absent and new exists, consider it idempotent.
        msg = str(err).lower()
        if "not found" in msg or "does not exist" in msg:
            return f"rename column {old} -> {new}: source already absent (idempotent)"
        raise
    return f"rename column {old} -> {new}"


def _alter_column(table: Table, action: exp.AlterColumn) -> str:
    name = action.args["this"].name
    dtype_node = action.args.get("dtype")
    if dtype_node is None:
        raise UnsupportedStatementError(
            statement_sql=action.sql(),
            reason="ALTER COLUMN without TYPE is not supported (only type changes are in v1)",
        )
    new_type = _iceberg_type_from_sqlglot(dtype_node)
    with table.update_schema() as us:
        us.update_column(name, field_type=new_type)
    return f"alter column {name} -> {new_type}"


# ---------------------------------------------------------------------------
# TBLPROPERTIES
# ---------------------------------------------------------------------------


def _set_tblproperties(table: Table, action: exp.AlterSet) -> str:
    # Pull the (key, value) pairs out of the AlterSet's nested Properties.
    props: dict[str, str] = {}
    for expression in action.args.get("expressions") or []:
        if isinstance(expression, exp.Properties):
            for prop in expression.args.get("expressions") or []:
                if isinstance(prop, exp.Property):
                    key = _scalar(prop.args.get("this"))
                    value = _scalar(prop.args.get("value"))
                    if key is None:
                        continue
                    props[key] = "" if value is None else value
    if not props:
        return "set tblproperties: (no keys parsed)"
    with table.transaction() as txn:
        # PyIceberg's set_properties takes a dict via the first positional
        # ``properties`` arg, NOT **kwargs of stringified Properties.
        txn.set_properties(properties=props)
    return f"set tblproperties {sorted(props)}"


def _unset_tblproperties(table: Table, action: UnsetTblProperties) -> str:
    keys: list[str] = []
    for k in action.args.get("expressions") or []:
        rendered = _scalar(k)
        if rendered is not None:
            keys.append(rendered)
    if not keys:
        return "unset tblproperties: (no keys parsed)"
    try:
        with table.transaction() as txn:
            txn.remove_properties(*keys)
    except (KeyError, ValueError) as err:
        # PyIceberg raises if the key isn't present. Treat as idempotent —
        # the user asked for the absence; we're delivering it.
        msg = str(err).lower()
        if "not found" in msg or "does not exist" in msg or "missing" in msg:
            return f"unset tblproperties {sorted(keys)}: already absent (idempotent)"
        raise
    return f"unset tblproperties {sorted(keys)}"


def _scalar(node: exp.Expression | None) -> str | None:
    """Render a literal-or-identifier node as a plain string.

    Used for property keys and values. Falls back to ``node.sql()`` when the
    node isn't a recognised scalar shape.
    """
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        return node.this
    if isinstance(node, exp.Identifier | exp.Column):
        return node.name
    return node.sql()


# ---------------------------------------------------------------------------
# Partition fields
# ---------------------------------------------------------------------------


def _add_partition_field(table: Table, action: AddPartitionField) -> str:
    source_column, transform, name = _resolve_partition_field(action)
    try:
        with table.update_spec() as us:
            us.add_field(source_column, transform, name)
    except ValueError as err:
        # PyIceberg refuses duplicate (source, transform) pairs even under
        # different partition-field names. Idempotent contract: log + continue.
        msg = str(err).lower()
        if "duplicate" in msg or "already" in msg:
            return (
                f"add partition field {source_column} ({transform}): already present (idempotent)"
            )
        raise
    return f"add partition field {source_column} ({transform})"


def _drop_partition_field(table: Table, action: DropPartitionField) -> str:
    name = _partition_field_name(action)
    try:
        with table.update_spec() as us:
            us.remove_field(name)
    except ValueError as err:
        if "not found" in str(err).lower() or "no such" in str(err).lower():
            return f"drop partition field {name}: already absent (idempotent)"
        raise
    return f"drop partition field {name}"


def _replace_partition_field(
    catalog: Catalog,
    table: Table,
    identifier: tuple[str, ...],
    action: ReplacePartitionField,
) -> str:
    # PyIceberg doesn't ship an atomic "replace field" call — sequence drop
    # then add. Both halves are idempotent under the contract above.
    old_name = _partition_field_name_from(action.args.get("this"))
    try:
        with table.update_spec() as us:
            us.remove_field(old_name)
    except ValueError as err:
        if not ("not found" in str(err).lower() or "no such" in str(err).lower()):
            raise
    table = catalog.load_table(identifier)
    transform_node = action.args.get("transform")
    new_alias = action.args.get("alias")
    if isinstance(transform_node, exp.Func):
        source = _column_name(transform_node)
        transform = _transform_from_func(transform_node)
    else:
        source = _column_name(transform_node)
        transform = IdentityTransform()
    new_name = new_alias.name if new_alias is not None else None
    try:
        with table.update_spec() as us:
            us.add_field(source, transform, new_name)
    except ValueError as err:
        if "duplicate" in str(err).lower() or "already" in str(err).lower():
            return (
                f"replace partition field {old_name} -> {source} ({transform}): "
                "target already present (idempotent)"
            )
        raise
    return f"replace partition field {old_name} -> {source} ({transform})"


def _resolve_partition_field(action: AddPartitionField) -> tuple[str, Transform, str | None]:
    """Decode ``AddPartitionField`` into ``(source_column, transform, name)``."""
    transform_node = action.args.get("transform")
    alias = action.args.get("alias")
    if isinstance(transform_node, exp.Func):
        source = _column_name(transform_node)
        transform = _transform_from_func(transform_node)
    else:
        # Bare-column ADD PARTITION FIELD x → identity transform.
        this = action.args.get("this")
        source = _column_name(this)
        transform = IdentityTransform()
    name = alias.name if alias is not None else None
    return source, transform, name


def _partition_field_name(action: DropPartitionField) -> str:
    transform = action.args.get("transform")
    if transform is not None:
        # ``DROP PARTITION FIELD bucket(16, x)``: name is the synthesised
        # ``<col>_bucket`` PyIceberg uses by default (or the alias, if set).
        return _column_name(transform)
    return _partition_field_name_from(action.args.get("this"))


def _partition_field_name_from(node: exp.Expression | None) -> str:
    if node is None:
        raise DispatchError("partition field reference is empty")
    if isinstance(node, exp.Func):
        return _column_name(node)
    return _column_name(node)


def _column_name(node: exp.Expression | None) -> str:
    """Best-effort: return the column name implied by *node*.

    Handles three shapes:

    * ``Column``/``Identifier`` → return ``.name`` directly.
    * Anonymous transform call like ``bucket(N, col)`` → walk
      ``expressions`` for the first column/identifier argument.
    * Typed-Func transform like ``year(col)`` which sqlglot lowers to
      ``Year(this=Cast(Column))`` → walk the ``this`` payload through any
      wrapping ``Cast`` to find the underlying column.
    """
    if node is None:
        raise DispatchError("column reference is empty")
    if isinstance(node, exp.Column | exp.Identifier):
        return node.name
    if isinstance(node, exp.Func):
        for arg in node.args.get("expressions") or []:
            if isinstance(arg, exp.Column | exp.Identifier):
                return arg.name
        this = node.args.get("this")
        if this is not None:
            return _column_name(_unwrap_cast(this))
    if isinstance(node, exp.Cast):
        return _column_name(node.args.get("this"))
    raise DispatchError(f"could not extract source column from {node.sql()}")


def _unwrap_cast(node: exp.Expression) -> exp.Expression:
    """Strip ``Cast(...)`` wrappers added by sqlglot for typed transforms."""
    while isinstance(node, exp.Cast):
        inner = node.args.get("this")
        if inner is None:
            return node
        node = inner
    return node


# Iceberg-Spark transform name → PyIceberg Transform mapping is inlined in
# _transform_from_func below. Each branch knows the arity of its target
# constructor; we avoid a generic table because ty can't see through the
# Transform alias to validate the per-constructor kwargs.


def _transform_from_func(node: exp.Func) -> Transform:
    """Translate an Iceberg-Spark transform call to its PyIceberg ``Transform``."""
    # sqlglot lowers typed time functions (Year, Month, Day, Hour) into
    # subclasses of exp.Func, where ``node.name`` is empty and the class
    # identity is what matters. Untyped/anonymous calls (bucket, truncate,
    # identity, years/months/days/hours when sqlglot doesn't recognise them
    # specifically) carry the function name in ``node.name`` directly.
    if isinstance(node, exp.Year):
        return YearTransform()
    if isinstance(node, exp.Month):
        return MonthTransform()
    if isinstance(node, exp.Day):
        return DayTransform()
    name = (node.name or _typed_func_name(node)).lower()
    if name == "bucket":
        return BucketTransform(num_buckets=_transform_int_arg(node))
    if name == "truncate":
        return TruncateTransform(width=_transform_int_arg(node))
    if name in ("year", "years"):
        return YearTransform()
    if name in ("month", "months"):
        return MonthTransform()
    if name in ("day", "days"):
        return DayTransform()
    if name in ("hour", "hours"):
        return HourTransform()
    if name == "identity":
        return IdentityTransform()
    raise UnsupportedStatementError(
        statement_sql=node.sql(),
        reason=(
            f"unsupported partition transform {name!r}; expected one of bucket, truncate, "
            "year(s), month(s), day(s), hour(s), identity"
        ),
    )


def _typed_func_name(node: exp.Func) -> str:
    """Render the name of a typed sqlglot Func node (uses ``sql_name`` when present)."""
    sql_name = getattr(node, "sql_name", None)
    if callable(sql_name):
        return sql_name()
    return type(node).__name__


def _transform_int_arg(node: exp.Func) -> int:
    """Pull the first numeric-literal argument from a transform function call."""
    first = (node.args.get("expressions") or [None])[0]
    if not isinstance(first, exp.Literal):
        raise DispatchError(
            f"{node.name}() requires a numeric literal as its first argument: {node.sql()}"
        )
    try:
        return int(first.this)
    except (TypeError, ValueError) as err:
        raise DispatchError(
            f"{node.name}() literal must be an integer; got {first.this!r}"
        ) from err


# ---------------------------------------------------------------------------
# WRITE ORDERED BY
# ---------------------------------------------------------------------------


def _write_ordered_by(table: Table, action: WriteOrderedBy) -> str:
    cols = action.args.get("expressions") or []
    parts: list[str] = []
    with table.update_sort_order() as uso:
        for item in cols:
            if isinstance(item, exp.Ordered):
                col_name = _column_name(item.args.get("this"))
                desc = bool(item.args.get("desc"))
            else:
                col_name = _column_name(item)
                desc = False
            # Per PyIceberg quirks memory: ``asc/desc`` require an explicit
            # transform argument in 0.11.1. We pass IdentityTransform() for
            # the simple column-sort case; user-supplied transform-in-sort
            # syntax is rare enough that we don't try to parse it here.
            if desc:
                uso.desc(col_name, IdentityTransform())
                parts.append(f"{col_name} DESC")
            else:
                uso.asc(col_name, IdentityTransform())
                parts.append(f"{col_name} ASC")
    return f"write ordered by ({', '.join(parts)})"


# ---------------------------------------------------------------------------
# INSERT VALUES
# ---------------------------------------------------------------------------


def _dispatch_insert(catalog: Catalog, stmt: exp.Insert) -> DispatchResult:
    expression = stmt.args.get("expression")
    if not isinstance(expression, exp.Values):
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason=(
                "only INSERT INTO ... VALUES (...) is supported in v1 — INSERT SELECT, "
                "MERGE, DELETE, and UPDATE are out of scope (see plan §2.2)"
            ),
        )
    table_node = stmt.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"INSERT missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    table = catalog.load_table(identifier)
    arrow = _values_to_arrow(expression, table)
    table.append(arrow)
    n_rows = arrow.num_rows
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=f"insert {n_rows} row(s) into {'.'.join(identifier)}",
    )


def _values_to_arrow(values: exp.Values, table: Table) -> pa.Table:
    """Build a PyArrow ``Table`` matching *table*'s Iceberg schema from VALUES.

    Per the PyIceberg quirks memory, PyArrow nullability is strict against
    ``required`` Iceberg fields — we must pass an explicit ``pa.schema`` whose
    per-field ``nullable=`` flags mirror the Iceberg ``required`` flags
    exactly. We derive the target schema from ``table.schema().as_arrow()``
    and run a ``cast(..., safe=True)`` against it so literals are coerced
    in-flight.
    """
    target = table.schema().as_arrow()
    iceberg_fields = table.schema().fields
    # Per-field rebuild with the Iceberg required flag as the source of truth.
    fields = [
        pa.field(t_field.name, t_field.type, nullable=not i_field.required)
        for t_field, i_field in zip(target, iceberg_fields, strict=True)
    ]
    target_schema = pa.schema(fields)

    rows = list(_value_rows(values, target_schema))
    arrow = pa.Table.from_pylist(rows, schema=target_schema)
    # `from_pylist` already coerces simple types; cast() with safe=True
    # surfaces any precision-losing conversion as a hard error instead of
    # silent truncation.
    return arrow.cast(target_schema, safe=True)


def _value_rows(values: exp.Values, schema: pa.Schema) -> Iterable[dict[str, object]]:
    """Yield row dicts matching *schema* from a parsed VALUES list.

    Each row is a Tuple of literals positionally aligned with the schema's
    fields. Strings, numbers, booleans, and NULLs are mapped to their
    Python equivalents; non-literal expressions raise.
    """
    names = list(schema.names)
    for tup in values.args.get("expressions") or []:
        if not isinstance(tup, exp.Tuple):
            raise DispatchError(
                f"each VALUES entry must be a tuple of literals, got {type(tup).__name__}"
            )
        literals = tup.args.get("expressions") or []
        if len(literals) != len(names):
            raise DispatchError(
                f"VALUES tuple has {len(literals)} fields but table has {len(names)} columns"
            )
        yield {names[i]: _literal_value(lit) for i, lit in enumerate(literals)}


def _literal_value(node: exp.Expression) -> object:
    """Convert a parsed literal node to its Python value.

    Supports: Literal (string/number), Boolean, Null. Identifiers/Columns
    inside a VALUES tuple are a user error (we'd be inserting a column
    reference, not data); raise.
    """
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        # Numeric literals: try int, fall back to float.
        text = node.this
        try:
            return int(text)
        except ValueError:
            pass
        try:
            return float(text)
        except ValueError as err:
            raise DispatchError(f"can't coerce literal {text!r} to a number") from err
    if isinstance(node, exp.Neg):
        inner = _literal_value(node.this)
        if isinstance(inner, int | float):
            return -inner
        raise DispatchError(f"can't negate non-numeric value {inner!r}")
    raise DispatchError(
        f"unsupported literal {type(node).__name__} in VALUES; only string/number/bool/NULL"
    )


# ---------------------------------------------------------------------------
# CREATE TABLE: column-def → Iceberg schema
# ---------------------------------------------------------------------------


def _build_iceberg_schema(column_defs: list[exp.Expression]) -> Schema:
    """Assemble an Iceberg ``Schema`` from a list of parsed ``ColumnDef`` nodes."""
    fields: list[NestedField] = []
    for idx, node in enumerate(column_defs, start=1):
        if not isinstance(node, exp.ColumnDef):
            raise DispatchError(f"unexpected node in column list: {type(node).__name__}")
        name = node.name
        kind = node.args.get("kind")
        iceberg_type = _iceberg_type_from_sqlglot(kind)
        # We assume nullable unless the column-def carries an explicit
        # NOT NULL constraint. Iceberg-Spark's ``required`` is rendered as a
        # ``NOT NULL`` constraint via constraints=[]; sqlglot puts that under
        # ``constraints``.
        required = _is_required(node)
        fields.append(
            NestedField(field_id=idx, name=name, field_type=iceberg_type, required=required)
        )
    return Schema(*fields)


def _is_required(col_def: exp.ColumnDef) -> bool:
    """Detect ``NOT NULL`` on a ColumnDef without scanning the rendered SQL."""
    for constraint in col_def.args.get("constraints") or []:
        # sqlglot stores NOT NULL as ColumnConstraint with kind=NotNullColumnConstraint.
        kind = constraint.args.get("kind") if isinstance(constraint, exp.Expression) else None
        if isinstance(kind, exp.NotNullColumnConstraint):
            return True
    return False


_SQLGLOT_TYPE_TO_ICEBERG: dict[exp.DataType.Type, type[IcebergType]] = {
    exp.DataType.Type.BOOLEAN: BooleanType,
    exp.DataType.Type.TINYINT: IntegerType,
    exp.DataType.Type.SMALLINT: IntegerType,
    exp.DataType.Type.INT: IntegerType,
    exp.DataType.Type.BIGINT: LongType,
    exp.DataType.Type.FLOAT: FloatType,
    exp.DataType.Type.DOUBLE: DoubleType,
    exp.DataType.Type.VARCHAR: StringType,
    exp.DataType.Type.TEXT: StringType,
    exp.DataType.Type.CHAR: StringType,
    exp.DataType.Type.BINARY: BinaryType,
    exp.DataType.Type.VARBINARY: BinaryType,
    exp.DataType.Type.DATE: DateType,
    exp.DataType.Type.TIMESTAMP: TimestampType,
    exp.DataType.Type.TIMESTAMPTZ: TimestamptzType,
    exp.DataType.Type.TIMESTAMPLTZ: TimestamptzType,
}


def _iceberg_type_from_sqlglot(kind: exp.Expression | None) -> IcebergType:
    """Translate a sqlglot DataType into the corresponding PyIceberg type.

    The v1 supported types are the Iceberg primitive types representable in
    Spark SQL. Composite types (struct, list, map) and DECIMAL with explicit
    precision/scale parse to a DataType with extended args — we handle the
    common DECIMAL case explicitly and reject the rest with
    :class:`UnsupportedStatementError` so the user knows what's missing.
    """
    if not isinstance(kind, exp.DataType):
        raise DispatchError(f"missing or unparseable column type: {kind!r}")
    base = kind.this
    if base == exp.DataType.Type.DECIMAL:
        # Pull precision/scale from the DataType's expressions list.
        params = kind.args.get("expressions") or []
        precision = _decimal_param(params, 0, default=38)
        scale = _decimal_param(params, 1, default=0)
        return DecimalType(precision=precision, scale=scale)
    iceberg_cls = _SQLGLOT_TYPE_TO_ICEBERG.get(base)
    if iceberg_cls is None:
        raise UnsupportedStatementError(
            statement_sql=kind.sql(),
            reason=(
                f"unsupported column type {base.name}; v1 covers the Iceberg primitive types "
                "only (int, bigint, float, double, string, boolean, date, timestamp/tz, "
                "decimal, binary)"
            ),
        )
    return iceberg_cls()


def _decimal_param(params: list[exp.Expression], idx: int, *, default: int) -> int:
    """Pull a DECIMAL(precision, scale) parameter; tolerate missing/malformed.

    The parameter is wrapped in a ``DataTypeParam`` whose ``this`` is itself
    a ``Literal`` carrying the numeric value as a string. The ``.name``
    attribute on ``DataTypeParam`` resolves to that string for us.
    """
    if idx >= len(params):
        return default
    node = params[idx]
    try:
        if isinstance(node, exp.DataTypeParam):
            inner = node.this
            if isinstance(inner, exp.Literal):
                return int(inner.this)
            return int(node.name)
        return int(node.name)
    except (TypeError, ValueError, AttributeError):
        return default
