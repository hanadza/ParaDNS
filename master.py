#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
master.py - Master untuk Distributed DNS Lookup (arsitektur Master-Worker)

Fitur (sesuai ketentuan UAS):
  a. Task Decomposition   : daftar domain dipecah menjadi task berukuran --chunk-size.
  b. Parallel Processing  : dikerjakan Worker lewat multiprocessing (lihat worker.py).
  c. Distributed Process  : komunikasi TCP (JSON per baris) Master <-> Worker.
  d. Dynamic Load Balance : --lb dynamic (pull-based: Worker cepat otomatis dapat lebih banyak task)
                            --lb static  (pembagian merata di awal, sebagai pembanding "sebelum").
  e. Fault Tolerance      : heartbeat timeout + deteksi koneksi putus + task timeout,
                            task gagal di-requeue (maks --max-retries), Recovery Time dicatat.
  f. Performance Eval     : Execution Time, Speedup, Efficiency, Throughput, CPU/Memory,
                            Recovery Time -> results/metrics_<tag>.json + results_<tag>.csv

Mode:
  --mode sequential   : baseline 1 proses, 1 thread (tanpa jaringan)
  --mode local        : multiprocessing di 1 komputer (tanpa jaringan)
  --mode distributed  : Master-Worker lewat jaringan (default)

Contoh:
  python3 master.py --mode sequential  --domains domains.txt --limit 20000
  python3 master.py --mode local       --domains domains.txt --limit 20000 --local-processes 4
  python3 master.py --mode distributed --domains domains.txt --limit 20000 --min-workers 9 --lb dynamic
  python3 master.py --mode distributed --domains domains.txt --limit 20000 --min-workers 9 --lb static
"""

import argparse
import csv
import json
import multiprocessing as mp
import os
import socket
import statistics
import sys
import threading
import time
from collections import Counter, deque

from worker import HAS_DNSPYTHON, resolve_chunk

try:
    import psutil
except ImportError:
    psutil = None


# ----------------------------------------------------------------------------
# Utilitas
# ----------------------------------------------------------------------------
def load_domains(path, limit=None):
    """Baca daftar domain. Mendukung 1 domain per baris atau format Tranco 'rank,domain'."""
    domains = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                line = line.split(",")[-1].strip()
            domains.append(line.lower())
            if limit and len(domains) >= limit:
                break
    return domains


def percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1))))
    return s[k]


class Sampler(threading.Thread):
    """Sampling CPU/Memory sistem (untuk mode sequential & local)."""

    def __init__(self, interval=1.0):
        super().__init__(daemon=True)
        self.interval = interval
        self.cpu, self.mem = [], []
        self.stop_event = threading.Event()

    def run(self):
        if not psutil:
            return
        psutil.cpu_percent(None)
        while not self.stop_event.wait(self.interval):
            self.cpu.append(psutil.cpu_percent(None))
            self.mem.append(psutil.virtual_memory().percent)

    def stop(self):
        self.stop_event.set()


def summarize_rows(rows):
    """rows: [domain, status, ips, latency_ms, worker]"""
    status = Counter(r[1] for r in rows)
    lat_ok = [r[3] for r in rows if r[1] == "OK"]
    return {
        "domains_processed": len(rows),
        "status_count": dict(status),
        "success_rate_pct": round(100.0 * status.get("OK", 0) / len(rows), 2) if rows else 0,
        "latency_ms_mean": round(statistics.mean(lat_ok), 2) if lat_ok else 0,
        "latency_ms_p95": round(percentile(lat_ok, 95), 2) if lat_ok else 0,
    }


def write_outputs(out_dir, tag, rows, metrics):
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"results_{tag}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "status", "ips", "latency_ms", "worker"])
        w.writerows(rows)
    json_path = os.path.join(out_dir, f"metrics_{tag}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"\n[Master] Hasil  : {csv_path}")
    print(f"[Master] Metrik : {json_path}")


def find_baseline(args):
    if args.baseline_time:
        return args.baseline_time
    path = os.path.join(args.output_dir, "metrics_sequential.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f).get("execution_time_s")
        except (OSError, ValueError):
            return None
    return None


def add_speedup(metrics, baseline, units, label="workers"):
    if not baseline:
        return
    total = metrics["execution_time_s"]
    sp = baseline / total if total else 0
    metrics["baseline_time_s"] = round(baseline, 3)
    metrics["speedup"] = round(sp, 3)
    metrics[f"efficiency_per_{label}_pct"] = round(100.0 * sp / units, 2) if units else None


# ----------------------------------------------------------------------------
# Mode SEQUENTIAL & LOCAL (tanpa jaringan)
# ----------------------------------------------------------------------------
def _local_job(arg):
    tid, doms, cfg, threads = arg
    return resolve_chunk(tid, doms, cfg, threads, 0.0)


def run_sequential(args, tasks, cfg):
    sampler = Sampler()
    sampler.start()
    t0 = time.perf_counter()
    rows = []
    for i, (tid, doms) in enumerate(tasks, 1):
        res = resolve_chunk(tid, doms, cfg, 1, 0.0)  # 1 proses, 1 thread
        rows.extend(r + ["sequential"] for r in res["results"])
        print(f"\r[Sequential] task {i}/{len(tasks)}", end="", flush=True)
    total = time.perf_counter() - t0
    sampler.stop()
    print()
    m = {
        "mode": "sequential",
        "execution_time_s": round(total, 3),
        "throughput_domain_per_s": round(len(rows) / total, 2) if total else 0,
        "n_tasks": len(tasks),
        "cpu_avg_pct": round(statistics.mean(sampler.cpu), 2) if sampler.cpu else None,
        "mem_avg_pct": round(statistics.mean(sampler.mem), 2) if sampler.mem else None,
    }
    m.update(summarize_rows(rows))
    return rows, m


def run_local(args, tasks, cfg):
    procs = args.local_processes
    jobs = [(tid, doms, cfg, args.threads) for tid, doms in tasks]
    t0 = time.perf_counter()
    rows = []
    with mp.Pool(procs) as pool:
        sampler = Sampler()
        sampler.start()
        for i, res in enumerate(pool.imap_unordered(_local_job, jobs), 1):
            rows.extend(r + [f"local-pid{res['pid']}"] for r in res["results"])
            print(f"\r[Local x{procs}] task {i}/{len(jobs)}", end="", flush=True)
        sampler.stop()
    total = time.perf_counter() - t0
    print()
    m = {
        "mode": "local",
        "processes": procs,
        "threads_per_process": args.threads,
        "execution_time_s": round(total, 3),
        "throughput_domain_per_s": round(len(rows) / total, 2) if total else 0,
        "n_tasks": len(jobs),
        "cpu_avg_pct": round(statistics.mean(sampler.cpu), 2) if sampler.cpu else None,
        "mem_avg_pct": round(statistics.mean(sampler.mem), 2) if sampler.mem else None,
    }
    m.update(summarize_rows(rows))
    add_speedup(m, find_baseline(args), procs, "process")
    return rows, m


# ----------------------------------------------------------------------------
# Mode DISTRIBUTED
# ----------------------------------------------------------------------------
class Task:
    __slots__ = ("id", "domains", "status", "attempts", "worker", "assigned_at", "done_by", "done_at")

    def __init__(self, tid, domains):
        self.id = tid
        self.domains = domains
        self.status = "pending"  # pending | inflight | done | failed
        self.attempts = 0
        self.worker = None
        self.assigned_at = None
        self.done_by = None
        self.done_at = None


class WorkerInfo:
    def __init__(self, wid, conn, addr, msg):
        self.id = wid
        self.sock = conn
        self.addr = addr[0]
        self.hostname = msg.get("hostname", "?")
        self.cpus = msg.get("cpus", 1)
        self.processes = msg.get("processes", 1)
        self.send_lock = threading.Lock()
        self.last_seen = time.time()
        self.alive = True
        self.inflight = set()
        self.tasks_done = 0
        self.domains_done = 0
        self.busy_time = 0.0
        self.last_result_at = None
        self.cpu, self.mem = [], []

    @property
    def label(self):
        return f"{self.hostname}#{self.id}"


class Master:
    def __init__(self, args, domains, cfg):
        self.args = args
        self.cfg = cfg
        self.lock = threading.RLock()
        self.max_attempts = args.max_retries + 1

        cs = args.chunk_size
        self.tasks = {}
        for i in range(0, len(domains), cs):
            tid = i // cs
            self.tasks[tid] = Task(tid, domains[i:i + cs])
        self.total_tasks = len(self.tasks)
        self.total_domains = len(domains)

        self.shared = deque()      # antrean bersama (dynamic) + task retry
        self.private = {}          # antrean per Worker (static)
        self.workers = {}
        self.next_wid = 1
        self.results = {}
        self.done_count = 0
        self.failed_count = 0
        self.domains_done = 0
        self.retry_count = 0
        self.events = []           # log kegagalan Worker

        self.started = threading.Event()
        self.finished = threading.Event()
        self.t0 = None
        self.t_end = None
        self.n_workers_at_start = 0

    # ---- server ------------------------------------------------------------------
    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.args.host, self.args.port))
        srv.listen(64)
        threading.Thread(target=self._accept_loop, args=(srv,), daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        print(f"[Master] Listening di {self.args.host}:{self.args.port} | "
              f"{self.total_domains} domain -> {self.total_tasks} task "
              f"(chunk={self.args.chunk_size}) | LB={self.args.lb}")

    def _accept_loop(self, srv):
        while True:
            try:
                conn, addr = srv.accept()
            except OSError:
                return
            threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()

    def _send_to(self, w, msg):
        data = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")
        try:
            with w.send_lock:
                w.sock.sendall(data)
            return True
        except OSError:
            return False

    def _handle_client(self, conn, addr):
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        rfile = conn.makefile("r", encoding="utf-8")
        w = None
        try:
            line = rfile.readline()
            if not line:
                return
            msg = json.loads(line)
            if msg.get("type") != "register":
                return
            w = self._register(conn, addr, msg)
            for line in rfile:
                msg = json.loads(line)
                w.last_seen = time.time()
                mtype = msg.get("type")
                if mtype == "request":
                    self._on_request(w)
                elif mtype == "result":
                    self._on_result(w, msg)
                elif mtype == "task_failed":
                    self._on_task_failed(w, msg)
                elif mtype == "heartbeat":
                    if msg.get("cpu") is not None:
                        w.cpu.append(msg["cpu"])
                    if msg.get("mem") is not None:
                        w.mem.append(msg["mem"])
        except (OSError, ValueError):
            pass
        finally:
            if w:
                self.mark_dead(w, "koneksi terputus")
            else:
                try:
                    conn.close()
                except OSError:
                    pass

    def _register(self, conn, addr, msg):
        with self.lock:
            wid = self.next_wid
            self.next_wid += 1
            w = WorkerInfo(wid, conn, addr, msg)
            self.workers[wid] = w
        self._send_to(w, {"type": "ack", "worker_id": wid, "config": self.cfg})
        print(f"[Master] Worker terdaftar: {w.label} ({w.addr}) | core={w.cpus} proses={w.processes}")
        return w

    # ---- penjadwalan ---------------------------------------------------------------
    def alive_workers(self):
        with self.lock:
            return [w for w in self.workers.values() if w.alive]

    def start(self):
        """Mulai pembagian task. Dipanggil setelah jumlah Worker minimum terpenuhi."""
        with self.lock:
            alive = sorted(self.alive_workers(), key=lambda x: x.id)
            self.n_workers_at_start = len(alive)
            ids = sorted(self.tasks)
            if self.args.lb == "dynamic":
                self.shared.extend(ids)
            else:  # static: bagi rata (round-robin) di awal, tanpa memperhatikan kecepatan Worker
                for w in alive:
                    self.private[w.id] = deque()
                for i, tid in enumerate(ids):
                    self.private[alive[i % len(alive)].id].append(tid)
            self.t0 = time.time()
            self.started.set()
        print(f"[Master] MULAI dengan {len(alive)} Worker.")

    def _next_task(self, w):
        for q in (self.shared, self.private.get(w.id)):
            while q:
                t = self.tasks[q.popleft()]
                if t.status == "pending":
                    return t
        return None

    def _on_request(self, w):
        with self.lock:
            if self.finished.is_set():
                reply = {"type": "shutdown"}
            elif not self.started.is_set():
                reply = {"type": "wait"}
            else:
                t = self._next_task(w)
                if t:
                    t.status = "inflight"
                    t.worker = w.id
                    t.assigned_at = time.time()
                    t.attempts += 1
                    w.inflight.add(t.id)
                    reply = {"type": "task", "task_id": t.id, "domains": t.domains, "attempt": t.attempts}
                else:
                    reply = {"type": "wait"}
        if not self._send_to(w, reply):
            self.mark_dead(w, "gagal mengirim data")

    def _on_result(self, w, msg):
        tid = msg["task_id"]
        with self.lock:
            w.inflight.discard(tid)
            t = self.tasks.get(tid)
            if t is None or t.status == "done":
                return  # hasil duplikat (task sempat di-requeue), abaikan
            if t.status == "failed":
                self.failed_count -= 1
            now = time.time()
            t.status = "done"
            t.done_by = w.label
            t.done_at = now
            self.results[tid] = msg["results"]
            self.done_count += 1
            self.domains_done += len(msg["results"])
            w.tasks_done += 1
            w.domains_done += len(msg["results"])
            w.busy_time += msg.get("elapsed", 0.0)
            w.last_result_at = now
            self._check_recovery(tid, now)
            self._check_finished()

    def _on_task_failed(self, w, msg):
        with self.lock:
            tid = msg["task_id"]
            w.inflight.discard(tid)
            t = self.tasks.get(tid)
            if t and t.status == "inflight" and t.worker == w.id:
                print(f"[Master] Task {tid} error di {w.label}: {msg.get('error')}")
                self._requeue(t, "error di worker")

    def _requeue(self, t, reason):
        """Kembalikan task ke antrean (atau tandai gagal permanen). Return True jika di-requeue."""
        if t.status != "inflight":
            return False
        if t.attempts >= self.max_attempts:
            t.status = "failed"
            self.failed_count += 1
            print(f"[Master] Task {t.id} GAGAL permanen ({reason}) setelah {t.attempts} percobaan.")
            self._check_finished()
            return False
        t.status = "pending"
        self.retry_count += 1
        self.shared.appendleft(t.id)
        return True

    def _check_finished(self):
        if not self.finished.is_set() and self.done_count + self.failed_count >= self.total_tasks:
            self.t_end = time.time()
            self.finished.set()

    # ---- fault tolerance -------------------------------------------------------------
    def mark_dead(self, w, reason):
        with self.lock:
            if not w.alive:
                return
            w.alive = False
            try:
                w.sock.close()
            except OSError:
                pass
            if self.finished.is_set():
                return  # shutdown normal, bukan kegagalan
            now = time.time()
            requeued = []
            for tid in list(w.inflight):
                t = self.tasks[tid]
                if t.status == "inflight" and t.worker == w.id and self._requeue(t, reason):
                    requeued.append(tid)
            w.inflight.clear()
            orphan = 0
            q = self.private.pop(w.id, None)  # static: antrean milik Worker yang mati
            if q:
                for tid in q:
                    if self.tasks[tid].status == "pending":
                        self.shared.append(tid)
                        orphan += 1
            event = {
                "worker": w.label,
                "reason": reason,
                "detected_at_s": round(now - self.t0, 3) if self.t0 else None,
                "detection_delay_s": round(now - w.last_seen, 3),
                "tasks_requeued": len(requeued),
                "orphan_tasks_redistributed": orphan,
                "_pending": set(requeued),
                "_detected": now,
                "recovery_time_s": None,
            }
            if not event["_pending"]:
                event["recovery_time_s"] = 0.0
            self.events.append(event)
            print(f"[Master] !! Worker {w.label} GAGAL ({reason}). "
                  f"{len(requeued)} task di-requeue, {orphan} task antrean dialihkan.")

    def _check_recovery(self, tid, now):
        for ev in self.events:
            if tid in ev["_pending"]:
                ev["_pending"].discard(tid)
                if not ev["_pending"] and ev["recovery_time_s"] is None:
                    ev["recovery_time_s"] = round(now - ev["_detected"], 3)

    def _monitor_loop(self):
        while not self.finished.is_set():
            time.sleep(1.0)
            now = time.time()
            with self.lock:
                for w in list(self.workers.values()):
                    if not w.alive:
                        continue
                    if now - w.last_seen > self.args.hb_timeout:
                        self.mark_dead(w, "heartbeat timeout")
                        continue
                    for tid in list(w.inflight):  # task timeout (Worker hidup tapi macet)
                        t = self.tasks[tid]
                        if t.status == "inflight" and t.worker == w.id \
                                and now - t.assigned_at > self.args.task_timeout:
                            w.inflight.discard(tid)
                            print(f"[Master] Task {tid} timeout di {w.label}, di-requeue.")
                            self._requeue(t, "task timeout")

    # ---- penutupan & metrik --------------------------------------------------------------
    def shutdown_workers(self):
        for w in self.alive_workers():
            self._send_to(w, {"type": "shutdown"})

    def progress_line(self):
        el = time.time() - self.t0
        with self.lock:
            return (f"[{el:7.1f}s] task {self.done_count}/{self.total_tasks} | gagal {self.failed_count} "
                    f"| retry {self.retry_count} | worker aktif {len(self.alive_workers())} "
                    f"| {self.domains_done / el if el else 0:8.0f} domain/dtk")

    def collect(self):
        total = self.t_end - self.t0
        rows = []
        for tid in sorted(self.tasks):
            t = self.tasks[tid]
            if t.status == "done":
                rows.extend(r + [t.done_by] for r in self.results[tid])

        per_worker = []
        for w in sorted(self.workers.values(), key=lambda x: x.id):
            per_worker.append({
                "worker": w.label,
                "addr": w.addr,
                "cpus": w.cpus,
                "processes": w.processes,
                "tasks_done": w.tasks_done,
                "domains_done": w.domains_done,
                "share_pct": round(100.0 * w.domains_done / max(1, len(rows)), 2),
                "busy_time_s": round(w.busy_time, 2),
                "pool_utilization_pct": round(100.0 * w.busy_time / (w.processes * total), 2) if total else 0,
                "finish_at_s": round(w.last_result_at - self.t0, 2) if w.last_result_at else None,
                "cpu_avg_pct": round(statistics.mean(w.cpu), 2) if w.cpu else None,
                "cpu_max_pct": round(max(w.cpu), 2) if w.cpu else None,
                "mem_avg_pct": round(statistics.mean(w.mem), 2) if w.mem else None,
                "alive_at_end": w.alive,
            })

        done_counts = [p["domains_done"] for p in per_worker if p["tasks_done"] > 0]
        finish = [p["finish_at_s"] for p in per_worker if p["finish_at_s"] is not None]
        events = [{k: v for k, v in ev.items() if not k.startswith("_")} for ev in self.events]

        m = {
            "mode": "distributed",
            "load_balancing": self.args.lb,
            "workers_at_start": self.n_workers_at_start,
            "total_processes_at_start": sum(p["processes"] for p in per_worker[:self.n_workers_at_start]),
            "chunk_size": self.args.chunk_size,
            "threads_per_process": self.cfg.get("threads", 1),
            "execution_time_s": round(total, 3),
            "throughput_domain_per_s": round(len(rows) / total, 2) if total else 0,
            "n_tasks": self.total_tasks,
            "tasks_failed_permanent": self.failed_count,
            "retry_count": self.retry_count,
            "load_imbalance_ratio": round(max(done_counts) / statistics.mean(done_counts), 3) if done_counts else None,
            "finish_time_spread_s": round(max(finish) - min(finish), 2) if finish else None,
            "failure_events": events,
            "per_worker": per_worker,
        }
        m.update(summarize_rows(rows))
        add_speedup(m, find_baseline(self.args), max(1, self.n_workers_at_start), "worker")
        return rows, m


def run_distributed(args, domains, cfg):
    master = Master(args, domains, cfg)
    master.serve()
    print(f"[Master] Menunggu minimal {args.min_workers} Worker terhubung...")
    waited = 0.0
    while len(master.alive_workers()) < args.min_workers:
        time.sleep(0.5)
        waited += 0.5
        if args.wait_timeout and waited >= args.wait_timeout:
            print("[Master] Timeout menunggu Worker. Lanjut dengan Worker yang ada.")
            break
        if int(waited * 2) % 10 == 0:
            print(f"\r[Master] Worker terhubung: {len(master.alive_workers())}/{args.min_workers}",
                  end="", flush=True)
    print()
    if not master.alive_workers():
        print("[Master] Tidak ada Worker. Keluar.")
        sys.exit(1)

    master.start()
    while not master.finished.wait(2.0):
        print(master.progress_line())
    master.shutdown_workers()
    time.sleep(1.0)
    print(master.progress_line())
    return master.collect()


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def print_summary(m):
    print("\n" + "=" * 64)
    print(f" RINGKASAN  mode={m['mode']}" + (f"  LB={m['load_balancing']}" if 'load_balancing' in m else ""))
    print("=" * 64)
    print(f" Domain diproses   : {m['domains_processed']}  (sukses {m['success_rate_pct']}%)")
    print(f" Execution time    : {m['execution_time_s']} detik")
    print(f" Throughput        : {m['throughput_domain_per_s']} domain/detik")
    if "speedup" in m:
        print(f" Speedup           : {m['speedup']}x  (baseline {m['baseline_time_s']} dtk)")
        for k, v in m.items():
            if k.startswith("efficiency_per_"):
                print(f" Efficiency        : {v}%  ({k.replace('efficiency_per_', 'per ').replace('_pct', '')})")
    else:
        print(" Speedup           : (jalankan --mode sequential dulu / pakai --baseline-time)")
    if m["mode"] == "distributed":
        print(f" Load imbalance    : {m['load_imbalance_ratio']} (1.0 = merata) | "
              f"selisih waktu selesai antar Worker: {m['finish_time_spread_s']} dtk")
        for ev in m["failure_events"]:
            print(f" Kegagalan         : {ev['worker']} ({ev['reason']}) | requeue {ev['tasks_requeued']} task "
                  f"| Recovery Time {ev['recovery_time_s']} dtk")
        print("-" * 64)
        print(f" {'Worker':<22}{'Task':>6}{'Domain':>9}{'Share%':>8}{'CPU%':>7}{'Mem%':>7}")
        for p in m["per_worker"]:
            print(f" {p['worker']:<22}{p['tasks_done']:>6}{p['domains_done']:>9}{p['share_pct']:>8}"
                  f"{str(p['cpu_avg_pct']):>7}{str(p['mem_avg_pct']):>7}")
    print("=" * 64)


def parse_args():
    p = argparse.ArgumentParser(description="Master - Distributed DNS Lookup")
    p.add_argument("--mode", choices=["distributed", "sequential", "local"], default="distributed")
    p.add_argument("--domains", required=True, help="File daftar domain (1 per baris atau 'rank,domain')")
    p.add_argument("--limit", type=int, default=0, help="Batasi jumlah domain (0 = semua)")
    p.add_argument("--chunk-size", type=int, default=500, help="Jumlah domain per task")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--min-workers", type=int, default=1, help="Mulai setelah N Worker terhubung")
    p.add_argument("--wait-timeout", type=float, default=0, help="Batas tunggu Worker (detik, 0 = tanpa batas)")
    p.add_argument("--lb", choices=["dynamic", "static"], default="dynamic")
    p.add_argument("--hb-timeout", type=float, default=6.0, help="Worker dianggap mati jika tak ada kabar (detik)")
    p.add_argument("--task-timeout", type=float, default=120.0, help="Batas waktu satu task (detik)")
    p.add_argument("--max-retries", type=int, default=3, help="Maks. percobaan ulang per task")
    p.add_argument("--dns-timeout", type=float, default=3.0, help="Timeout per query DNS (detik)")
    p.add_argument("--nameserver", default="", help="DNS server tujuan (mis. 8.8.8.8). Kosong = bawaan OS")
    p.add_argument("--record", default="A", help="Tipe record DNS (A, AAAA, MX, ...)")
    p.add_argument("--threads", type=int, default=1, help="Thread per proses Worker")
    p.add_argument("--local-processes", type=int, default=os.cpu_count() or 1, help="Jumlah proses mode local")
    p.add_argument("--output-dir", default="results")
    p.add_argument("--tag", default="", help="Nama berkas keluaran (default otomatis)")
    p.add_argument("--baseline-time", type=float, default=0,
                   help="Waktu sequential (detik) untuk hitung speedup. Default: baca results/metrics_sequential.json")
    return p.parse_args()


def main():
    args = parse_args()
    domains = load_domains(args.domains, args.limit)
    if not domains:
        print("Daftar domain kosong.")
        sys.exit(1)
    cfg = {"timeout": args.dns_timeout, "nameserver": args.nameserver,
           "record": args.record, "threads": args.threads}
    print(f"[Master] {len(domains)} domain dimuat | dnspython={'ya' if HAS_DNSPYTHON else 'TIDAK (pakai socket)'}")

    cs = args.chunk_size
    tasks = [(i // cs, domains[i:i + cs]) for i in range(0, len(domains), cs)]

    if args.mode == "sequential":
        rows, metrics = run_sequential(args, tasks, cfg)
        tag = args.tag or "sequential"
    elif args.mode == "local":
        rows, metrics = run_local(args, tasks, cfg)
        tag = args.tag or f"local_p{args.local_processes}"
    else:
        rows, metrics = run_distributed(args, domains, cfg)
        tag = args.tag or f"distributed_{args.lb}_w{metrics['workers_at_start']}"

    metrics["n_domains_input"] = len(domains)
    metrics["dns_timeout_s"] = args.dns_timeout
    metrics["nameserver"] = args.nameserver or "default OS"
    print_summary(metrics)
    write_outputs(args.output_dir, tag, rows, metrics)


if __name__ == "__main__":
    mp.freeze_support()
    main()
