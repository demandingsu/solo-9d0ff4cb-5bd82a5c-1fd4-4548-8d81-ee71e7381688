"""SQLite persistence with transactional, versioned cell reports."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Optional, Sequence, Tuple

from .algorithm import Cycle, detect_cycles
from .diagnostics import compute_diagnostics

SCHEMA = """
CREATE TABLE IF NOT EXISTS cells (
    cell_id      TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS samples (
    cell_id      TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    current_ma   INTEGER NOT NULL,
    PRIMARY KEY (cell_id, ts)
);

CREATE TABLE IF NOT EXISTS acq_logs (
    cell_id      TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    ts           INTEGER NOT NULL,
    PRIMARY KEY (cell_id, seq)
);

CREATE TABLE IF NOT EXISTS report_versions (
    cell_id      TEXT NOT NULL,
    version      INTEGER NOT NULL,
    total_cycles INTEGER NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (cell_id, version)
);

CREATE TABLE IF NOT EXISTS cycles (
    cell_id       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    cycle_no      INTEGER NOT NULL,
    charge_ah     REAL NOT NULL,
    discharge_ah  REAL NOT NULL,
    started_at    INTEGER NOT NULL,
    ended_at      INTEGER NOT NULL,
    PRIMARY KEY (cell_id, version, cycle_no)
);

CREATE TABLE IF NOT EXISTS upload_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cell_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    detail       TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


class ConflictError(Exception):
    """Same (cell, ts) already exists with a different current value."""

    def __init__(self, ts: int, existing: float, incoming: float):
        self.ts = ts
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"conflict at ts={ts}: existing={existing}A incoming={incoming}A"
        )


class ImportError_(Exception):
    """Generic import failure (bad CSV payload etc.)."""


class LogConflictError(Exception):
    """Same (cell, seq) already stored with a different UTC second."""

    def __init__(self, seq: int, existing_ts: int, incoming_ts: int):
        self.seq = seq
        self.existing_ts = existing_ts
        self.incoming_ts = incoming_ts
        super().__init__(
            f"log conflict at seq={seq}: existing ts={existing_ts}"
            f" incoming ts={incoming_ts}"
        )


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def _conn(self):
        conn = _connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # Import
    # ------------------------------------------------------------------ #
    def import_batch(
        self, cell_id: str, rows: Iterable[Tuple[int, float]]
    ) -> dict:
        """Atomically import a CSV batch.

        Returns a summary dict. Raises ConflictError / ImportError_ on
        rejection; in that case nothing is committed (old samples and the
        previous report stay untouched).
        """
        # Normalize + validate the payload before opening the transaction.
        incoming: list[tuple[int, float]] = []
        seen: dict[int, float] = {}
        try:
            for ts, current in rows:
                ts = int(ts)
                current = float(current)
                if ts < 0:
                    raise ValueError(f"negative timestamp {ts}")
                if not (current == current):  # NaN
                    raise ValueError("NaN current")
                if ts in seen and seen[ts] != current:
                    raise ImportError_(
                        f"batch itself contains conflicting values at ts={ts}"
                    )
                seen[ts] = current
                incoming.append((ts, current))
        except (TypeError, ValueError) as exc:
            raise ImportError_(f"invalid row: {exc}") from exc

        if not incoming:
            raise ImportError_("CSV contains no data rows")

        conn = _connect(self.db_path)
        try:
            # One writer at a time. BEGIN IMMEDIATE acquires the reserved
            # lock up front so concurrent uploads serialize instead of
            # hitting SQLITE_BUSY mid-transaction.
            conn.execute("BEGIN IMMEDIATE")

            # Reject whole batch on any (cell, ts) value mismatch.
            for ts, current in incoming:
                row = conn.execute(
                    "SELECT current_ma FROM samples WHERE cell_id=? AND ts=?",
                    (cell_id, ts),
                ).fetchone()
                if row is not None and row["current_ma"] != _to_storage(current):
                    raise ConflictError(ts, row["current_ma"] / 1_000_000.0, current)

            inserted = 0
            duplicate = 0
            for ts, current in incoming:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO samples(cell_id, ts, current_ma)"
                    " VALUES(?,?,?)",
                    (cell_id, ts, _to_storage(current)),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    duplicate += 1

            conn.execute(
                "INSERT INTO cells(cell_id) VALUES(?) ON CONFLICT(cell_id)"
                " DO UPDATE SET updated_at="
                "strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (cell_id,),
            )

            # Recompute the report from the full committed sample set.
            all_rows = conn.execute(
                "SELECT ts, current_ma FROM samples WHERE cell_id=?"
                " ORDER BY ts ASC",
                (cell_id,),
            ).fetchall()
            samples = [(r["ts"], r["current_ma"] / 1_000_000.0) for r in all_rows]
            cycles = detect_cycles(samples)

            version_row = conn.execute(
                "SELECT COALESCE(MAX(version),0) AS v FROM report_versions"
                " WHERE cell_id=?",
                (cell_id,),
            ).fetchone()
            version = version_row["v"] + 1

            conn.execute(
                "INSERT INTO report_versions(cell_id, version, total_cycles)"
                " VALUES(?,?,?)",
                (cell_id, version, len(cycles)),
            )
            conn.executemany(
                "INSERT INTO cycles(cell_id, version, cycle_no, charge_ah,"
                " discharge_ah, started_at, ended_at) VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        cell_id,
                        version,
                        c.cycle_no,
                        c.charge_ah,
                        c.discharge_ah,
                        c.started_at,
                        c.ended_at,
                    )
                    for c in cycles
                ],
            )
            conn.execute(
                "INSERT INTO upload_log(cell_id, status, detail) VALUES(?,?,?)",
                (cell_id, "accepted", f"version={version}"),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            try:
                with self._conn() as log_conn:
                    log_conn.execute(
                        "INSERT INTO upload_log(cell_id, status, detail)"
                        " VALUES(?,?,?)",
                        (cell_id, "rejected", "batch rejected; no data changed"),
                    )
            except Exception:
                pass
            raise
        finally:
            conn.close()

        return {
            "cell_id": cell_id,
            "inserted": inserted,
            "duplicate": duplicate,
            "version": version,
            "total_cycles": len(cycles),
        }

    def import_logs(
        self, cell_id: str, entries: Iterable[Tuple[int, int]]
    ) -> dict:
        """Atomically import a batch of acquisition-log entries (seq, ts).

        Entries may arrive out of order and in any number of batches.
        Re-uploading an already stored (seq, ts) pair is idempotent; a seq
        already stored with a *different* timestamp rejects the whole batch
        (LogConflictError) and nothing is committed.
        """
        # Normalize + validate the payload before opening the transaction.
        incoming: list[tuple[int, int]] = []
        seen: dict[int, int] = {}
        try:
            for seq, ts in entries:
                seq = int(seq)
                ts = int(ts)
                if seq < 0:
                    raise ValueError(f"negative sequence number {seq}")
                if ts < 0:
                    raise ValueError(f"negative timestamp {ts}")
                if seq in seen and seen[seq] != ts:
                    raise ImportError_(
                        f"batch itself contains conflicting timestamps"
                        f" at seq={seq}"
                    )
                seen[seq] = ts
                incoming.append((seq, ts))
        except (TypeError, ValueError) as exc:
            raise ImportError_(f"invalid log entry: {exc}") from exc

        if not incoming:
            raise ImportError_("batch contains no log entries")

        conn = _connect(self.db_path)
        try:
            # One writer at a time, same serialization as sample imports.
            conn.execute("BEGIN IMMEDIATE")

            # Reject whole batch on any (cell, seq) timestamp mismatch.
            for seq, ts in incoming:
                row = conn.execute(
                    "SELECT ts FROM acq_logs WHERE cell_id=? AND seq=?",
                    (cell_id, seq),
                ).fetchone()
                if row is not None and row["ts"] != ts:
                    raise LogConflictError(seq, row["ts"], ts)

            inserted = 0
            duplicate = 0
            for seq, ts in incoming:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO acq_logs(cell_id, seq, ts)"
                    " VALUES(?,?,?)",
                    (cell_id, seq, ts),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    duplicate += 1

            conn.execute(
                "INSERT INTO cells(cell_id) VALUES(?) ON CONFLICT(cell_id)"
                " DO UPDATE SET updated_at="
                "strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (cell_id,),
            )
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM acq_logs WHERE cell_id=?",
                (cell_id,),
            ).fetchone()["n"]
            conn.execute(
                "INSERT INTO upload_log(cell_id, status, detail) VALUES(?,?,?)",
                (
                    cell_id,
                    "accepted",
                    f"acq_logs inserted={inserted} duplicate={duplicate}",
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            try:
                with self._conn() as log_conn:
                    log_conn.execute(
                        "INSERT INTO upload_log(cell_id, status, detail)"
                        " VALUES(?,?,?)",
                        (cell_id, "rejected",
                         "acq_logs batch rejected; no data changed"),
                    )
            except Exception:
                pass
            raise
        finally:
            conn.close()

        return {
            "cell_id": cell_id,
            "inserted": inserted,
            "duplicate": duplicate,
            "total_logs": total,
        }

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get_report(self, cell_id: str) -> Optional[dict]:
        """Return report + cycles of the latest committed version.

        The version row, cycle details and total are all read from the same
        committed version; a single snapshot transaction guarantees they
        belong to one state even if an upload commits concurrently.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                cell = conn.execute(
                    "SELECT cell_id FROM cells WHERE cell_id=?", (cell_id,)
                ).fetchone()
                if cell is None:
                    return None
                ver = conn.execute(
                    "SELECT version, total_cycles, created_at FROM"
                    " report_versions WHERE cell_id=? ORDER BY version DESC"
                    " LIMIT 1",
                    (cell_id,),
                ).fetchone()
                if ver is None:
                    return {
                        "cell_id": cell_id,
                        "version": 0,
                        "total_cycles": 0,
                        "cycles": [],
                        "created_at": None,
                    }
                cycle_rows = conn.execute(
                    "SELECT cycle_no, charge_ah, discharge_ah, started_at,"
                    " ended_at FROM cycles WHERE cell_id=? AND version=?"
                    " ORDER BY cycle_no ASC",
                    (cell_id, ver["version"]),
                ).fetchall()
                return {
                    "cell_id": cell_id,
                    "version": ver["version"],
                    "total_cycles": ver["total_cycles"],
                    "created_at": ver["created_at"],
                    "cycles": [dict(r) for r in cycle_rows],
                }
            finally:
                conn.execute("COMMIT")

    def get_diagnostics(self, cell_id: str) -> Optional[dict]:
        """Acquisition-log QC diagnostics from one committed snapshot.

        Log rows, raw-sample timestamps and the latest report version with
        its cycles are all read inside a single transaction, so the
        findings, the cycle associations and the returned report_version
        always belong to the same committed state — even when log or
        sample uploads commit concurrently.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                cell = conn.execute(
                    "SELECT cell_id FROM cells WHERE cell_id=?", (cell_id,)
                ).fetchone()
                if cell is None:
                    return None
                log_rows = conn.execute(
                    "SELECT seq, ts FROM acq_logs WHERE cell_id=?"
                    " ORDER BY seq ASC",
                    (cell_id,),
                ).fetchall()
                sample_rows = conn.execute(
                    "SELECT ts FROM samples WHERE cell_id=?", (cell_id,)
                ).fetchall()
                ver = conn.execute(
                    "SELECT version, total_cycles FROM report_versions"
                    " WHERE cell_id=? ORDER BY version DESC LIMIT 1",
                    (cell_id,),
                ).fetchone()
                version = 0
                total_cycles = 0
                cycle_rows = []
                if ver is not None:
                    version = ver["version"]
                    total_cycles = ver["total_cycles"]
                    cycle_rows = conn.execute(
                        "SELECT cycle_no, started_at, ended_at FROM cycles"
                        " WHERE cell_id=? AND version=? ORDER BY cycle_no ASC",
                        (cell_id, version),
                    ).fetchall()
            finally:
                conn.execute("COMMIT")

        diag = compute_diagnostics(
            [(r["seq"], r["ts"]) for r in log_rows],
            {r["ts"] for r in sample_rows},
            [(c["cycle_no"], c["started_at"], c["ended_at"]) for c in cycle_rows],
        )
        diag.update(
            {
                "cell_id": cell_id,
                "report_version": version,
                "total_cycles": total_cycles,
                "log_count": len(log_rows),
                "sample_count": len(sample_rows),
            }
        )
        return diag

    def list_cells(self) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT c.cell_id, c.created_at, c.updated_at,"
                " COALESCE(v.total_cycles,0) AS total_cycles,"
                " COALESCE(v.version,0) AS version FROM cells c"
                " LEFT JOIN report_versions v"
                " ON v.cell_id=c.cell_id AND v.version="
                " (SELECT MAX(version) FROM report_versions WHERE cell_id=c.cell_id)"
                " ORDER BY c.cell_id"
            ).fetchall()
            return [dict(r) for r in rows]


def _to_storage(current_a: float) -> int:
    """Store amperes as integer microamps.

    CSV currents like 1.234 A are stored as integer microamps so equality
    checks for idempotency are exact (no float text round-trip surprises).
    Precision below 1 µA is rejected by rounding consistency on read.
    """
    return round(float(current_a) * 1_000_000)
