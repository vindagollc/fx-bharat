"""MySQL backend strategy."""

from __future__ import annotations

from fx_bharat.db.relational_backend import RelationalBackend


class MySQLBackend(RelationalBackend):
    """Concrete relational backend for MySQL engines."""

    pass


__all__ = ["MySQLBackend"]
