"""PostgreSQL backend strategy."""

from __future__ import annotations

from fx_bharat.db.relational_backend import RelationalBackend


class PostgresBackend(RelationalBackend):
    """Concrete relational backend for PostgreSQL engines."""

    pass


__all__ = ["PostgresBackend"]
