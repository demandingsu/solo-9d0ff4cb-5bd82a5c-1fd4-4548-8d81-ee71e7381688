"""Live-server smoke test.

Runs against an already-started uvicorn (default http://127.0.0.1:8000).
Covers: late-point rewrite/renumbering, gap truncation, idempotency and
conflict rejection, concurrent uploads/queries with snapshot consistency,
and acquisition-log QC diagnostics (out-of-order batches, late fills,
rollbacks, missing samples, cycle association, interleaved queries).
"""

import concurrent.futures
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def upload(client, cell, rows, header="timestamp,current"):
    text = header + "\n" + "".join(f"{ts},{cur}\n" for ts, cur in rows)
    return client.post(
        f"{BASE}/cells/{cell}/samples",
        files={"file": ("batch.csv", text, "text/csv")},
    )


def post_logs(client, cell, entries):
    return client.post(
        f"{BASE}/cells/{cell}/logs",
        json={"logs": [{"seq": s, "ts": t} for s, t in entries]},
    )


def diagnostics(client, cell):
    return client.get(f"{BASE}/cells/{cell}/diagnostics").json()


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  PASS: {msg}")


def main():
    with httpx.Client(timeout=30) as c:
        r = c.get(f"{BASE}/health")
        check(r.status_code == 200, "health endpoint")

        # --- 1. late point rewrites/renumbers cycles --------------------
        cell = "LIVE-LATE"
        r = upload(c, cell, [(0, 0), (10, 1), (20, 0), (30, -1), (40, 0), (50, 1)])
        check(r.status_code == 200, "first batch accepted")
        check(r.json()["total_cycles"] == 1, "one closed cycle, tail ignored")
        v1 = r.json()["version"]

        r = upload(c, cell, [(60, 0), (70, -1), (80, 0)])
        check(r.status_code == 200, "late batch accepted")
        check(r.json()["version"] == v1 + 1, "report version bumped")
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 2, "late point closed second cycle")
        check([x["cycle_no"] for x in rep["cycles"]] == [1, 2], "cycles renumbered 1..N")
        check(
            abs(rep["cycles"][0]["charge_ah"] - 1 * 10 / 3600) < 1e-12,
            "charge Ah from left-value integral",
        )

        # --- 2. gap > 60 s truncates ------------------------------------
        cell = "LIVE-GAP"
        r = upload(c, cell, [(0, 0), (10, 2), (71, 0), (80, -3), (90, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 0, "61 s gap truncates the sequence")

        r = upload(c, cell, [(0, 0), (60, 2), (120, 0), (130, -1), (140, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 1, "exactly-60 s intervals are kept")

        # --- 3. idempotency + conflict whole-batch rejection ------------
        cell = "LIVE-IDEM"
        rows = [(0, 0), (10, 1.5), (20, 0), (30, -2.5), (40, 0)]
        upload(c, cell, rows)
        r = upload(c, cell, rows)
        check(r.json()["inserted"] == 0 and r.json()["duplicate"] == 5,
              "identical re-upload is idempotent")
        before = c.get(f"{BASE}/cells/{cell}/report").json()
        r = upload(c, cell, [(10, 9.9), (99, 1)])
        check(r.status_code == 409, "different value at same ts -> 409")
        after = c.get(f"{BASE}/cells/{cell}/report").json()
        check(after["version"] == before["version"], "rejected batch keeps old version")
        check(after["total_cycles"] == before["total_cycles"], "old report unchanged")

        # --- 4. concurrency: 8 writers, parallel readers ----------------
        cell = "LIVE-CONC"

        def writer(n):
            base = n * 1000
            with httpx.Client(timeout=30) as wc:
                return upload(
                    wc, cell,
                    [(base, 0), (base + 10, 1), (base + 20, 0),
                     (base + 30, -1), (base + 40, 0)],
                ).status_code

        def reader(_):
            bad = []
            with httpx.Client(timeout=30) as rc:
                for _ in range(40):
                    rr = rc.get(f"{BASE}/cells/{cell}/report")
                    if rr.status_code == 404:
                        continue
                    rep = rr.json()
                    nos = [x["cycle_no"] for x in rep["cycles"]]
                    if rep["total_cycles"] != len(rep["cycles"]) or nos != list(
                        range(1, rep["total_cycles"] + 1)
                    ):
                        bad.append(rep)
            return bad

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            f_w = [pool.submit(writer, n) for n in range(8)]
            f_r = [pool.submit(reader, n) for n in range(8)]
            codes = [f.result() for f in f_w]
            inconsistencies = [x for f in f_r for x in f.result()]
        check(codes == [200] * 8, f"all 8 concurrent writers committed (got {codes})")
        check(not inconsistencies, "every query saw one internally consistent state")
        final = c.get(f"{BASE}/cells/{cell}/report").json()
        check(final["total_cycles"] == 8, "final report contains all 8 cycles")

        cells = c.get(f"{BASE}/cells").json()
        check(any(x["cell_id"] == cell for x in cells), "cell list endpoint")

        # --- 5. acquisition logs + QC diagnostics -----------------------
        cell = "LIVE-QC"
        r = upload(c, cell, [(0, 0), (10, 1), (20, 0), (30, -1), (40, 0)])
        check(r.status_code == 200, "QC cell: one cycle of samples accepted")

        # out-of-order batches: 5..6 first, then 1..2 -> gap 3..4
        r = post_logs(c, cell, [(5, 30), (6, 40)])
        check(r.status_code == 200 and r.json()["inserted"] == 2,
              "log batch (seq 5..6) accepted first")
        r = post_logs(c, cell, [(1, 0), (2, 10)])
        check(r.json()["inserted"] == 2 and r.json()["total_logs"] == 4,
              "out-of-order log batch (seq 1..2) accepted")
        d = diagnostics(c, cell)
        check(d["summary"]["gaps"] == 1 and d["summary"]["missing_frames"] == 2,
              "gap between arrived seqs reported as missing frames")
        gap = d["missing_frame_gaps"][0]
        check(gap["missing_seqs"] == [3, 4], "missing seqs listed explicitly")
        check(gap["cycles_before"] == [1] and gap["cycles_after"] == [1],
              "gap ends associated with current cycle 1")
        check(d["report_version"] >= 1, "diagnostics carry committed report version")

        # late logs fill the gap
        r = post_logs(c, cell, [(3, 20), (4, 25)])
        check(r.status_code == 200, "late log batch accepted")
        d = diagnostics(c, cell)
        check(d["summary"]["gaps"] == 0, "late logs eliminated the gap")

        # idempotent re-upload
        r = post_logs(c, cell, [(3, 20), (4, 25)]).json()
        check(r["inserted"] == 0 and r["duplicate"] == 2,
              "same (seq, ts) re-upload is idempotent")

        # same seq, different ts -> whole batch rejected, old state kept
        before = diagnostics(c, cell)
        r = post_logs(c, cell, [(4, 26), (9, 90)])
        check(r.status_code == 409, "same seq different ts -> 409")
        after = diagnostics(c, cell)
        check(after["log_count"] == before["log_count"] == 6,
              "rejected log batch keeps old diagnostics")

        # clock rollback + missing raw samples
        r = post_logs(c, cell, [(7, 35)])
        check(r.status_code == 200, "rollback log accepted")
        d = diagnostics(c, cell)
        check(d["summary"]["clock_rollbacks"] == 1, "clock rollback detected")
        rb = d["clock_rollbacks"][0]
        check((rb["seq_before"], rb["seq_after"]) == (6, 7)
              and rb["rollback_seconds"] == 5,
              "rollback between adjacent arrived records (40s -> 35s)")
        missing = {m["seq"]: m for m in d["missing_samples"]}
        check(set(missing) == {4, 7}, "log timestamps without raw samples")
        check(missing[4]["cycles"] == [1] and missing[7]["cycles"] == [1],
              "missing-sample entries located in cycle 1")

        # backfilling samples clears missing-sample entries and re-cuts the
        # cycles, which re-associates the rollback ends
        r = upload(c, cell, [(25, 0), (35, 0), (50, 0), (60, 1), (70, 0),
                             (80, -1), (90, 0)])
        check(r.status_code == 200, "sample backfill accepted")
        d = diagnostics(c, cell)
        check(d["report_version"] == 2, "report version bumped by backfill")
        check(d["total_cycles"] == 2, "backfill re-cut the cycles (now 2)")
        check(d["summary"]["missing_samples"] == 0,
              "backfilled samples cleared all missing-sample entries")
        rb = d["clock_rollbacks"][0]
        check(rb["cycles_before"] == [2] and rb["cycles_after"] == [1, 2],
              "rollback ends re-associated after cycle re-cut "
              "(shared boundary zero belongs to both cycles)")

        # a new gap whose later end lands inside cycle 2
        r = post_logs(c, cell, [(9, 60), (11, 70)])
        check(r.status_code == 200, "logs inside cycle 2 accepted")
        d = diagnostics(c, cell)
        gaps = {g["seq_before"]: g for g in d["missing_frame_gaps"]}
        check(set(gaps) == {7, 9}, "two gaps between arrived seqs")
        check(gaps[9]["missing_seqs"] == [10]
              and gaps[9]["cycles_before"] == [2]
              and gaps[9]["cycles_after"] == [2],
              "gap ends associated with new cycle 2")

        # fill one gap (ts 45 has no raw sample but sits inside cycle 2),
        # then an unlocated log timestamp outside every cycle
        post_logs(c, cell, [(8, 45)])
        post_logs(c, cell, [(20, 500)])
        d = diagnostics(c, cell)
        missing = {m["seq"]: m for m in d["missing_samples"]}
        check(set(missing) == {8, 20}, "missing samples after gap fill")
        check(missing[8]["cycles"] == [2], "ts=45 located in cycle 2")
        check(missing[20]["cycles"] == [],
              "ts=500 outside all cycles -> unlocated")
        check(d["summary"]["clock_rollbacks"] == 1,
              "no phantom rollbacks from out-of-order arrival")

        # --- 6. interleaved log/sample uploads and diagnostics queries --
        cell = "LIVE-QC-CONC"

        def log_writer(n):
            base = n * 100
            with httpx.Client(timeout=30) as wc:
                return post_logs(
                    wc, cell,
                    [(base + i, base * 10 + i * 10) for i in range(1, 6)],
                ).status_code

        def diag_reader(_):
            bad = []
            last_count = -1
            with httpx.Client(timeout=30) as rc:
                for _ in range(30):
                    rr = rc.get(f"{BASE}/cells/{cell}/diagnostics")
                    if rr.status_code == 404:
                        continue
                    d = rr.json()
                    s = d["summary"]
                    if d["log_count"] < last_count:
                        bad.append("log_count went backwards")
                    last_count = d["log_count"]
                    if (s["gaps"] != len(d["missing_frame_gaps"])
                            or s["clock_rollbacks"] != len(d["clock_rollbacks"])
                            or s["missing_samples"] != len(d["missing_samples"])
                            or s["missing_frames"] != sum(
                                g["missing_count"]
                                for g in d["missing_frame_gaps"])):
                        bad.append("summary inconsistent with detail lists")
                    for g in d["missing_frame_gaps"]:
                        if not all(g["seq_before"] < m < g["seq_after"]
                                   for m in g["missing_seqs"]):
                            bad.append("missing seq outside its gap")
            return bad

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            f_w = [pool.submit(log_writer, n) for n in range(4)]
            f_r = [pool.submit(diag_reader, n) for n in range(4)]
            codes = [f.result() for f in f_w]
            bad = [x for f in f_r for x in f.result()]
        check(codes == [200] * 4, "4 concurrent log writers committed")
        check(not bad, "interleaved diagnostics queries always consistent")
        d = diagnostics(c, cell)
        check(d["log_count"] == 20 and d["summary"]["gaps"] == 3,
              "final diagnostics reflect all committed log batches")

    print("\nALL LIVE CHECKS PASSED")


if __name__ == "__main__":
    for attempt in range(20):
        try:
            httpx.get(f"{BASE}/health", timeout=2)
            break
        except httpx.HTTPError:
            time.sleep(0.5)
    main()
