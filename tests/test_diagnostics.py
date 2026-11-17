"""Tests for acquisition-log upload and QC diagnostics.

Covers: out-of-order batched arrival, idempotent re-upload, whole-batch
rejection on timestamp conflict, missing-frame/rollback/missing-sample
findings, cycle association of anomaly ends, late logs filling gaps and
exposing rollbacks, sample backfill changing findings and cycle numbers,
and interleaved concurrent uploads/queries snapshot consistency.
"""

import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="battery-qc-test-")
_DB = os.path.join(_TMP, "test.db")
for _suffix in ("", "-wal", "-shm"):
    try:
        os.remove(_DB + _suffix)
    except FileNotFoundError:
        pass
os.environ["BATTERY_DB_PATH"] = _DB

from fastapi.testclient import TestClient  # noqa: E402

from app.diagnostics import MISSING_SEQ_CAP, compute_diagnostics  # noqa: E402
from app.main import app  # noqa: E402


def csv_text(rows, header="timestamp,current\n"):
    return header + "".join(f"{ts},{cur}\n" for ts, cur in rows)


def upload_samples(client, cell_id, rows):
    return client.post(
        f"/cells/{cell_id}/samples",
        files={"file": ("samples.csv", csv_text(rows), "text/csv")},
    )


def upload_logs(client, cell_id, entries):
    return client.post(
        f"/cells/{cell_id}/logs",
        json={"logs": [{"seq": s, "ts": t} for s, t in entries]},
    )


# --------------------------------------------------------------------- #
# Pure computation
# --------------------------------------------------------------------- #
class ComputeDiagnosticsTests(unittest.TestCase):
    def test_gap_only_between_arrived_seqs(self):
        # arrived seqs 2,3,7: only 4..6 reported; nothing below 2 / above 7
        d = compute_diagnostics([(2, 100), (3, 110), (7, 150)], set(), [])
        self.assertEqual(len(d["missing_frame_gaps"]), 1)
        gap = d["missing_frame_gaps"][0]
        self.assertEqual((gap["seq_before"], gap["seq_after"]), (3, 7))
        self.assertEqual(gap["missing_count"], 3)
        self.assertEqual(gap["missing_seqs"], [4, 5, 6])
        self.assertFalse(gap["truncated"])
        self.assertEqual(d["summary"]["gaps"], 1)
        self.assertEqual(d["summary"]["missing_frames"], 3)

    def test_dense_logs_have_no_gap(self):
        d = compute_diagnostics([(1, 10), (2, 20), (3, 30)], set(), [])
        self.assertEqual(d["missing_frame_gaps"], [])
        self.assertEqual(d["summary"]["missing_frames"], 0)

    def test_unsorted_input_is_sorted_by_seq(self):
        d = compute_diagnostics([(7, 150), (2, 100), (3, 110)], set(), [])
        self.assertEqual(d["missing_frame_gaps"][0]["missing_seqs"], [4, 5, 6])

    def test_rollback_between_adjacent_arrived_records(self):
        d = compute_diagnostics([(1, 100), (2, 90), (4, 80)], set(), [])
        self.assertEqual(len(d["clock_rollbacks"]), 2)
        first, second = d["clock_rollbacks"]
        self.assertEqual((first["seq_before"], first["seq_after"]), (1, 2))
        self.assertEqual(first["rollback_seconds"], 10)
        self.assertEqual((second["seq_before"], second["seq_after"]), (2, 4))
        self.assertEqual(second["rollback_seconds"], 10)

    def test_equal_timestamps_are_not_rollbacks(self):
        d = compute_diagnostics([(1, 100), (2, 100)], set(), [])
        self.assertEqual(d["clock_rollbacks"], [])

    def test_missing_samples_and_cycle_association(self):
        cycles = [(1, 0, 40), (2, 40, 80)]
        d = compute_diagnostics(
            [(1, 10), (2, 40), (3, 999)], sample_ts={10}, cycles=cycles
        )
        ms = d["missing_samples"]
        self.assertEqual([(m["seq"], m["ts"]) for m in ms], [(2, 40), (3, 999)])
        # ts=40 is the shared boundary zero of cycles 1 and 2
        self.assertEqual(ms[0]["cycles"], [1, 2])
        # ts=999 is outside every committed cycle -> unlocated
        self.assertEqual(ms[1]["cycles"], [])

    def test_gap_ends_associated_with_cycles(self):
        cycles = [(1, 0, 40), (2, 40, 80)]
        d = compute_diagnostics([(1, 30), (5, 50)], {30, 50}, cycles)
        gap = d["missing_frame_gaps"][0]
        self.assertEqual(gap["cycles_before"], [1])
        self.assertEqual(gap["cycles_after"], [2])

    def test_huge_gap_list_is_capped_but_count_exact(self):
        d = compute_diagnostics([(1, 10), (100000, 20)], set(), [])
        gap = d["missing_frame_gaps"][0]
        self.assertEqual(gap["missing_count"], 99998)
        self.assertEqual(len(gap["missing_seqs"]), MISSING_SEQ_CAP)
        self.assertEqual(gap["missing_seqs"][0], 2)
        self.assertTrue(gap["truncated"])

    def test_empty_logs(self):
        d = compute_diagnostics([], set(), [])
        self.assertEqual(d["summary"], {
            "gaps": 0,
            "missing_frames": 0,
            "clock_rollbacks": 0,
            "missing_samples": 0,
        })


# --------------------------------------------------------------------- #
# HTTP: upload semantics
# --------------------------------------------------------------------- #
class LogUploadTests(unittest.TestCase):
    def setUp(self):
        self.c = TestClient(app)

    def test_upload_and_idempotent_reupload(self):
        cell = "CELL-QC-UP"
        r = upload_logs(self.c, cell, [(3, 30), (1, 10), (2, 20)])
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["inserted"], 3)
        self.assertEqual(body["duplicate"], 0)
        self.assertEqual(body["total_logs"], 3)

        r2 = upload_logs(self.c, cell, [(1, 10), (2, 20), (3, 30)])
        self.assertEqual(r2.status_code, 200)
        body2 = r2.json()
        self.assertEqual(body2["inserted"], 0)
        self.assertEqual(body2["duplicate"], 3)
        self.assertEqual(body2["total_logs"], 3)

    def test_same_seq_different_ts_rejects_whole_batch(self):
        cell = "CELL-QC-CONFLICT"
        r = upload_logs(self.c, cell, [(1, 10), (2, 20)])
        self.assertEqual(r.status_code, 200)

        # seq 2 collides with a different ts; seq 9 is brand new
        r2 = upload_logs(self.c, cell, [(2, 21), (9, 90)])
        self.assertEqual(r2.status_code, 409, r.text)
        self.assertIn("no data changed", r2.json()["detail"])

        # old state untouched: the brand-new seq 9 must not exist
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["log_count"], 2)
        r3 = upload_logs(self.c, cell, [(9, 90)])
        self.assertEqual(r3.json()["inserted"], 1)

    def test_batch_with_internal_conflict_rejected(self):
        cell = "CELL-QC-SELFCONFLICT"
        r = upload_logs(self.c, cell, [(1, 10), (1, 11)])
        self.assertEqual(r.status_code, 400)
        self.assertEqual(
            self.c.get(f"/cells/{cell}/diagnostics").status_code, 404
        )

    def test_empty_and_invalid_batches(self):
        cell = "CELL-QC-INVALID"
        r = self.c.post(f"/cells/{cell}/logs", json={"logs": []})
        self.assertEqual(r.status_code, 422)
        r = self.c.post(
            f"/cells/{cell}/logs", json={"logs": [{"seq": -1, "ts": 5}]}
        )
        self.assertEqual(r.status_code, 422)
        r = self.c.post(
            f"/cells/{cell}/logs", json={"logs": [{"seq": 1, "ts": -5}]}
        )
        self.assertEqual(r.status_code, 422)

    def test_diagnostics_unknown_cell_404(self):
        self.assertEqual(
            TestClient(app).get("/cells/NOPE-QC/diagnostics").status_code, 404
        )


# --------------------------------------------------------------------- #
# HTTP: findings evolve with late logs and sample backfill
# --------------------------------------------------------------------- #
class DiagnosticsFlowTests(unittest.TestCase):
    def setUp(self):
        self.c = TestClient(app)

    def test_out_of_order_batches_and_late_fill(self):
        cell = "CELL-QC-LATE"
        # batches arrive out of order: 5..6 first, then 1..2
        self.assertEqual(upload_logs(self.c, cell, [(5, 50), (6, 60)]).status_code, 200)
        self.assertEqual(upload_logs(self.c, cell, [(1, 10), (2, 20)]).status_code, 200)

        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["log_count"], 4)
        self.assertEqual(d["summary"]["gaps"], 1)
        gap = d["missing_frame_gaps"][0]
        self.assertEqual((gap["seq_before"], gap["seq_after"]), (2, 5))
        self.assertEqual(gap["missing_seqs"], [3, 4])

        # late batch fills the gap -> finding disappears
        r = upload_logs(self.c, cell, [(4, 40), (3, 30)])
        self.assertEqual(r.status_code, 200)
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["summary"]["gaps"], 0)
        self.assertEqual(d["summary"]["missing_frames"], 0)

    def test_late_log_exposes_rollbacks(self):
        cell = "CELL-QC-ROLLBACK"
        upload_logs(self.c, cell, [(1, 100), (3, 90)])
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        # adjacent arrived records (1,3): one rollback, plus gap of seq 2
        self.assertEqual(d["summary"]["clock_rollbacks"], 1)
        self.assertEqual(d["missing_frame_gaps"][0]["missing_seqs"], [2])

        # late record between them splits the finding into two rollbacks
        upload_logs(self.c, cell, [(2, 95)])
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["summary"]["gaps"], 0)
        self.assertEqual(d["summary"]["clock_rollbacks"], 2)
        pairs = [
            (r["seq_before"], r["seq_after"]) for r in d["clock_rollbacks"]
        ]
        self.assertEqual(pairs, [(1, 2), (2, 3)])

    def test_missing_samples_and_backfill(self):
        cell = "CELL-QC-BACKFILL"
        upload_logs(self.c, cell, [(1, 0), (2, 10), (3, 20)])

        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["report_version"], 0)  # no samples yet
        self.assertEqual(d["summary"]["missing_samples"], 3)
        self.assertTrue(all(m["cycles"] == [] for m in d["missing_samples"]))

        # samples for ts 0 and 10 only
        upload_samples(self.c, cell, [(0, 0.0), (10, 1.0)])
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual([m["seq"] for m in d["missing_samples"]], [3])

        # backfill the missing second -> list empties
        upload_samples(self.c, cell, [(20, 0.0)])
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["summary"]["missing_samples"], 0)

    def test_cycle_association_and_renumbering(self):
        cell = "CELL-QC-CYCLES"
        # cycle 1 spans [0, 40]
        upload_samples(
            self.c, cell, [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)]
        )
        upload_logs(self.c, cell, [(1, 10), (2, 30), (3, 50), (4, 999)])

        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["report_version"], 1)
        self.assertEqual(d["total_cycles"], 1)
        ms = {m["seq"]: m for m in d["missing_samples"]}
        # ts 50 and 999 have no raw sample; 50 is outside cycle 1 too
        self.assertEqual(ms[3]["cycles"], [])
        self.assertEqual(ms[4]["cycles"], [])

        # add a second cycle [50, 90]; log seq 3 (ts 50) now sits on the
        # opening zero of cycle 2
        upload_samples(
            self.c,
            cell,
            [(50, 0.0), (60, 1.0), (70, 0.0), (80, -1.0), (90, 0.0)],
        )
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["total_cycles"], 2)
        self.assertEqual(d["summary"]["missing_samples"], 1)  # only ts 999
        self.assertEqual(d["missing_samples"][0]["seq"], 4)

        # gap between seq 4 (ts 999) and a new far log end: both unlocated
        upload_logs(self.c, cell, [(6, 2000)])
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        gap = d["missing_frame_gaps"][0]
        self.assertEqual((gap["seq_before"], gap["seq_after"]), (4, 6))
        self.assertEqual(gap["missing_seqs"], [5])
        self.assertEqual(gap["cycles_before"], [])
        self.assertEqual(gap["cycles_after"], [])

        # rollback entry carries cycle association of both ends
        upload_logs(self.c, cell, [(5, 35)])  # fills gap, ts 35 < ts 999
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["summary"]["gaps"], 0)
        rb = d["clock_rollbacks"][0]
        self.assertEqual((rb["seq_before"], rb["seq_after"]), (4, 5))
        self.assertEqual(rb["rollback_seconds"], 999 - 35)
        self.assertEqual(rb["cycles_before"], [])   # ts 999: unlocated
        self.assertEqual(rb["cycles_after"], [1])   # ts 35 inside cycle 1

    def test_failed_upload_keeps_old_diagnostics(self):
        cell = "CELL-QC-KEEP"
        upload_samples(
            self.c, cell, [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)]
        )
        upload_logs(self.c, cell, [(1, 0), (2, 10), (4, 40)])
        before = self.c.get(f"/cells/{cell}/diagnostics").json()

        # conflicting log batch -> 409, diagnostics unchanged
        r = upload_logs(self.c, cell, [(2, 11), (5, 50)])
        self.assertEqual(r.status_code, 409)
        after = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(after, before)

        # conflicting sample batch -> 409, diagnostics unchanged
        r = upload_samples(self.c, cell, [(10, 9.9), (50, 1.0)])
        self.assertEqual(r.status_code, 409)
        after = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(after, before)

    def test_diagnostics_report_version_matches_report(self):
        cell = "CELL-QC-VERSION"
        upload_samples(
            self.c, cell, [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)]
        )
        upload_logs(self.c, cell, [(1, 10)])
        upload_samples(self.c, cell, [(50, 0.0)])  # bumps report version
        rep = self.c.get(f"/cells/{cell}/report").json()
        d = self.c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["report_version"], rep["version"])
        self.assertEqual(d["total_cycles"], rep["total_cycles"])


# --------------------------------------------------------------------- #
# Concurrency: interleaved log/sample uploads and diagnostics queries
# --------------------------------------------------------------------- #
class InterleavedConcurrencyTests(unittest.TestCase):
    def test_interleaved_uploads_and_queries(self):
        cell = "CELL-QC-CONC"
        errors = []
        barrier = threading.Barrier(8)

        def sample_writer(n):
            try:
                barrier.wait()
                c = TestClient(app)
                base = n * 1000
                rows = [
                    (base + 0, 0.0),
                    (base + 10, 1.0),
                    (base + 20, 0.0),
                    (base + 30, -1.0),
                    (base + 40, 0.0),
                ]
                r = upload_samples(c, cell, rows)
                if r.status_code != 200:
                    errors.append(f"sample writer {n}: {r.status_code} {r.text}")
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        def log_writer(n):
            try:
                barrier.wait()
                c = TestClient(app)
                base = n * 1000
                entries = [
                    (n * 100 + 1, base + 10),
                    (n * 100 + 2, base + 20),
                    (n * 100 + 3, base + 30),
                ]
                r = upload_logs(c, cell, entries)
                if r.status_code != 200:
                    errors.append(f"log writer {n}: {r.status_code} {r.text}")
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        def reader():
            try:
                barrier.wait()
                c = TestClient(app)
                last_log_count = -1
                for _ in range(40):
                    rr = c.get(f"/cells/{cell}/diagnostics")
                    if rr.status_code == 404:
                        continue  # first upload not committed yet
                    d = rr.json()
                    # logs are only ever inserted: count cannot go backwards
                    if d["log_count"] < last_log_count:
                        errors.append("log_count went backwards")
                    last_log_count = d["log_count"]
                    s = d["summary"]
                    if s["gaps"] != len(d["missing_frame_gaps"]):
                        errors.append("summary/gap list mismatch")
                    if s["clock_rollbacks"] != len(d["clock_rollbacks"]):
                        errors.append("summary/rollback list mismatch")
                    if s["missing_samples"] != len(d["missing_samples"]):
                        errors.append("summary/missing-sample list mismatch")
                    if s["missing_frames"] != sum(
                        g["missing_count"] for g in d["missing_frame_gaps"]
                    ):
                        errors.append("missing_frames total mismatch")
                    for g in d["missing_frame_gaps"]:
                        for m in g["missing_seqs"]:
                            if not (g["seq_before"] < m < g["seq_after"]):
                                errors.append("missing seq outside its gap")
                    # every associated cycle must exist in the *same*
                    # committed report version carried by this response
                    referenced = [
                        no
                        for g in d["missing_frame_gaps"]
                        for no in g["cycles_before"] + g["cycles_after"]
                    ] + [
                        no
                        for r in d["clock_rollbacks"]
                        for no in r["cycles_before"] + r["cycles_after"]
                    ] + [m2 for m in d["missing_samples"] for m2 in m["cycles"]]
                    for no in referenced:
                        if not (1 <= no <= d["total_cycles"]):
                            errors.append(
                                f"cycle {no} outside report version "
                                f"{d['report_version']} ({d['total_cycles']} cycles)"
                            )
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        threads = [
            threading.Thread(target=sample_writer, args=(n,)) for n in range(2)
        ]
        threads += [threading.Thread(target=log_writer, args=(n,)) for n in range(2)]
        threads += [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])

        # final state: 2 cycles, 6 logs, every log ts covered by a sample
        c = TestClient(app)
        rep = c.get(f"/cells/{cell}/report").json()
        d = c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["report_version"], rep["version"])
        self.assertEqual(d["total_cycles"], 2)
        self.assertEqual(d["log_count"], 6)
        self.assertEqual(d["summary"]["missing_samples"], 0)
        self.assertEqual(d["summary"]["clock_rollbacks"], 0)
        # one gap between the two writers' seq ranges (3 -> 101)
        self.assertEqual(d["summary"]["gaps"], 1)
        gap = d["missing_frame_gaps"][0]
        self.assertEqual((gap["seq_before"], gap["seq_after"]), (3, 101))
        self.assertEqual(gap["cycles_before"], [1])
        self.assertEqual(gap["cycles_after"], [2])

    def test_concurrent_conflicting_log_uploads_one_rejected(self):
        cell = "CELL-QC-CONC-CONFLICT"
        c = TestClient(app)
        self.assertEqual(upload_logs(c, cell, [(1, 10)]).status_code, 200)

        outcomes = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def push(ts):
            barrier.wait()
            cli = TestClient(app)
            r = upload_logs(cli, cell, [(1, ts), (2, 20)])
            with lock:
                outcomes.append(r.status_code)

        t1 = threading.Thread(target=push, args=(10,))  # same as stored
        t2 = threading.Thread(target=push, args=(11,))  # collides at seq 1
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(sorted(outcomes), [200, 409])
        d = c.get(f"/cells/{cell}/diagnostics").json()
        self.assertEqual(d["log_count"], 2)  # exactly one batch committed


if __name__ == "__main__":
    unittest.main(verbosity=2)
