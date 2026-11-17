"""Acquisition-log quality-control diagnostics.

Pure computation over one committed snapshot of a cell's state:

  * logs:    (seq, ts) pairs — instrument acquisition log (sequence number,
             UTC seconds), deduplicated by seq
  * samples: set of raw-sample timestamps (UTC seconds) stored for the cell
  * cycles:  (cycle_no, started_at, ended_at) rows of the latest committed
             cycle report

Findings:

  * missing frames: sequence numbers absent *between two arrived* seqs.
    Ranges before the first / after the last arrived seq are never reported
    (they may simply not have arrived yet).
  * clock rollbacks: adjacent *arrived* records (in seq order) whose UTC
    seconds go backwards (ts_after < ts_before).
  * missing raw samples: log timestamps with no raw sample at that second.

Every anomaly end is associated with the current cycle(s) whose
[started_at, ended_at] range covers its timestamp. An empty list means
"unlocated" (no committed cycle covers that timestamp). A timestamp on a
shared boundary zero belongs to both adjacent cycles and yields both.

Late log batches can fill gaps (findings disappear) or expose new
rollbacks; backfilled samples change the missing-sample list and the cycle
associations, because everything is recomputed from the current state.
"""

from __future__ import annotations

from typing import Dict, List, Sequence, Set, Tuple

# cap on the explicit missing-seq list per gap (the exact count is always
# reported in missing_count, even when the list is truncated)
MISSING_SEQ_CAP = 256

LogRow = Tuple[int, int]         # (seq, ts)
CycleRow = Tuple[int, int, int]  # (cycle_no, started_at, ended_at)


def cycles_at(ts: int, cycles: Sequence[CycleRow]) -> List[int]:
    """Numbers of the cycles whose [started_at, ended_at] range covers ts."""
    return [no for no, start, end in cycles if start <= ts <= end]


def compute_diagnostics(
    logs: Sequence[LogRow],
    sample_ts: Set[int],
    cycles: Sequence[CycleRow],
) -> Dict:
    """Compute QC findings for one cell from one committed snapshot."""
    ordered = sorted(logs)  # seq is unique, so this sorts by seq

    gaps: List[Dict] = []
    rollbacks: List[Dict] = []
    total_missing = 0

    for (seq_a, ts_a), (seq_b, ts_b) in zip(ordered, ordered[1:]):
        if seq_b - seq_a > 1:
            count = seq_b - seq_a - 1
            total_missing += count
            listed = list(range(seq_a + 1, min(seq_b, seq_a + 1 + MISSING_SEQ_CAP)))
            gaps.append(
                {
                    "seq_before": seq_a,
                    "seq_after": seq_b,
                    "missing_count": count,
                    "missing_seqs": listed,
                    "truncated": count > len(listed),
                    "cycles_before": cycles_at(ts_a, cycles),
                    "cycles_after": cycles_at(ts_b, cycles),
                }
            )
        if ts_b < ts_a:
            rollbacks.append(
                {
                    "seq_before": seq_a,
                    "ts_before": ts_a,
                    "seq_after": seq_b,
                    "ts_after": ts_b,
                    "rollback_seconds": ts_a - ts_b,
                    "cycles_before": cycles_at(ts_a, cycles),
                    "cycles_after": cycles_at(ts_b, cycles),
                }
            )

    missing_samples = [
        {"seq": seq, "ts": ts, "cycles": cycles_at(ts, cycles)}
        for seq, ts in ordered
        if ts not in sample_ts
    ]

    return {
        "summary": {
            "gaps": len(gaps),
            "missing_frames": total_missing,
            "clock_rollbacks": len(rollbacks),
            "missing_samples": len(missing_samples),
        },
        "missing_frame_gaps": gaps,
        "clock_rollbacks": rollbacks,
        "missing_samples": missing_samples,
    }
