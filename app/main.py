"""FastAPI application: battery cell sample upload and cycle reports."""

from __future__ import annotations

import os

from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from .csv_import import CSVParseError, parse_samples_csv
from .db import ConflictError, Database, ImportError_, LogConflictError

DB_PATH = os.environ.get("BATTERY_DB_PATH", "/data/battery.db")

app = FastAPI(
    title="Battery Cell Cycle Capacity API",
    version="1.1.0",
    description=(
        "Upload UTC-second current samples per cell. Zero current marks "
        "segment boundaries; only zero→positive→zero→negative→zero sequences "
        "count as cycles. Gaps over 60 s truncate a sequence. Ah is the "
        "left-value current integral over each interval. "
        "Acquisition QC: upload (sequence number, UTC seconds) instrument "
        "logs per cell and query diagnostics for missing frames, clock "
        "rollbacks and log timestamps without a matching raw sample, with "
        "anomaly ends associated to the current cycle numbers."
    ),
)
db = Database(DB_PATH)


class CycleOut(BaseModel):
    cycle_no: int
    charge_ah: float
    discharge_ah: float
    started_at: int
    ended_at: int


class ReportOut(BaseModel):
    cell_id: str
    version: int
    total_cycles: int
    created_at: str | None
    cycles: list[CycleOut]


class UploadOut(BaseModel):
    cell_id: str
    inserted: int
    duplicate: int
    version: int
    total_cycles: int


class LogEntryIn(BaseModel):
    seq: int = Field(ge=0, description="Acquisition sequence number")
    ts: int = Field(ge=0, description="UTC seconds of the acquisition")


class LogBatchIn(BaseModel):
    logs: list[LogEntryIn] = Field(
        min_length=1,
        description="Batch of (seq, ts) log entries; any order, any batching",
    )


class LogUploadOut(BaseModel):
    cell_id: str
    inserted: int
    duplicate: int
    total_logs: int


class MissingFrameGapOut(BaseModel):
    seq_before: int
    seq_after: int
    missing_count: int
    missing_seqs: list[int] = Field(
        description="Explicit missing sequence numbers (capped; see truncated)"
    )
    truncated: bool = Field(
        description="True when missing_seqs was capped; missing_count is exact"
    )
    cycles_before: list[int] = Field(
        description="Current cycle numbers covering the earlier end; "
        "empty = unlocated"
    )
    cycles_after: list[int] = Field(
        description="Current cycle numbers covering the later end; "
        "empty = unlocated"
    )


class ClockRollbackOut(BaseModel):
    seq_before: int
    ts_before: int
    seq_after: int
    ts_after: int
    rollback_seconds: int
    cycles_before: list[int] = Field(
        description="Current cycle numbers covering the earlier record; "
        "empty = unlocated"
    )
    cycles_after: list[int] = Field(
        description="Current cycle numbers covering the later record; "
        "empty = unlocated"
    )


class MissingSampleOut(BaseModel):
    seq: int
    ts: int
    cycles: list[int] = Field(
        description="Current cycle numbers covering this log timestamp; "
        "empty = unlocated"
    )


class DiagnosticsSummaryOut(BaseModel):
    gaps: int
    missing_frames: int
    clock_rollbacks: int
    missing_samples: int


class DiagnosticsOut(BaseModel):
    cell_id: str
    report_version: int = Field(
        description="Committed report version the cycle associations "
        "were computed from (same snapshot as the findings)"
    )
    total_cycles: int
    log_count: int
    sample_count: int
    summary: DiagnosticsSummaryOut
    missing_frame_gaps: list[MissingFrameGapOut]
    clock_rollbacks: list[ClockRollbackOut]
    missing_samples: list[MissingSampleOut]


@app.post(
    "/cells/{cell_id}/samples",
    response_model=UploadOut,
    tags=["upload"],
    summary="Upload a CSV batch of (UTC seconds, current A) samples",
)
async def upload_samples(cell_id: str, file: UploadFile = File(...)):
    content = await file.read()
    try:
        rows = parse_samples_csv(content)
        result = db.import_batch(cell_id, rows)
    except CSVParseError as exc:
        raise HTTPException(status_code=400, detail=f"CSV error: {exc}") from exc
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"batch rejected, no data changed: same timestamp {exc.ts} "
                f"already stored with {exc.existing} A, incoming {exc.incoming} A"
            ),
        ) from exc
    return result


@app.get("/cells", tags=["query"], summary="List all cells with latest report info")
def list_cells():
    return db.list_cells()


@app.post(
    "/cells/{cell_id}/logs",
    response_model=LogUploadOut,
    tags=["qc"],
    summary="Upload a batch of acquisition-log entries (seq, UTC seconds)",
)
def upload_logs(cell_id: str, batch: LogBatchIn):
    """Ingest acquisition-log entries for one cell.

    Entries may arrive out of order and split across any number of
    batches. Re-uploading an already stored (seq, ts) pair is idempotent;
    a seq already stored with a different UTC second rejects the whole
    batch (HTTP 409) and leaves previously stored logs untouched.
    """
    try:
        return db.import_logs(cell_id, [(e.seq, e.ts) for e in batch.logs])
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LogConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"batch rejected, no data changed: same seq {exc.seq} "
                f"already stored with ts {exc.existing_ts}, "
                f"incoming {exc.incoming_ts}"
            ),
        ) from exc


@app.get(
    "/cells/{cell_id}/diagnostics",
    response_model=DiagnosticsOut,
    tags=["qc"],
    summary="QC diagnostics: missing frames, clock rollbacks, missing samples",
)
def get_diagnostics(cell_id: str):
    """Recompute acquisition QC findings from the current committed state.

    * Missing frames: sequence numbers absent between two arrived seqs.
    * Clock rollbacks: adjacent arrived records (by seq) whose UTC
      seconds go backwards.
    * Missing samples: log timestamps with no raw sample at that second.

    Anomaly ends carry the current cycle numbers covering their
    timestamps (empty list = unlocated). Findings, raw-sample state and
    report_version are read from one committed snapshot.
    """
    diag = db.get_diagnostics(cell_id)
    if diag is None:
        raise HTTPException(status_code=404, detail=f"unknown cell {cell_id}")
    return diag


@app.get(
    "/cells/{cell_id}/report",
    response_model=ReportOut,
    tags=["query"],
    summary="Latest committed cycle report for one cell",
)
def get_report(cell_id: str):
    report = db.get_report(cell_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"unknown cell {cell_id}")
    return report


@app.get("/health", tags=["ops"], summary="Liveness probe")
def health():
    return {"status": "ok"}
