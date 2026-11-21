"""PostgreSQL baselines: what a typical production pipeline does today."""
from __future__ import annotations

from . import register
from .base import Dataset, Phases
from .pg_common import PostgresBase, copy_parquet, create_table_sql, index_a_sql, index_b_sql


@register
class PgTruncateCopy(PostgresBase):
    """Baseline B: one transaction, TRUNCATE + single-stream COPY into the table
    with both indexes in place. Readers block for the whole load (TRUNCATE takes
    ACCESS EXCLUSIVE), every row is inserted into two B-trees in random order."""

    name = "pg_truncate_copy"
    strategy = "TRUNCATE + single COPY into the indexed live table (one transaction)"
    atomic_swap = False

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.connect(autocommit=False) as c:
            if not self.table_exists(c):
                c.execute(create_table_sql("big_table"))
                c.execute(index_a_sql("big_table"))
                c.execute(index_b_sql("big_table"))
                c.commit()
            with phases("truncate"):
                c.execute("TRUNCATE big_table")
            with phases("copy_with_indexes"):
                copy_parquet(c, "big_table", ds.base_files)
            with phases("commit"):
                c.commit()
        with self.connect() as c, phases("analyze"):
            c.execute("ANALYZE big_table")
