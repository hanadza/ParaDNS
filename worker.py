#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
worker.py - Worker untuk Distributed DNS Lookup (arsitektur Master-Worker)

Tugas Worker:
  1. Terhubung ke Master (TCP) dan mendaftar beserta jumlah core/proses.
  2. Meminta task (potongan daftar domain) secara dinamis (pull-based).
  3. Memproses task secara paralel memakai multiprocessing.Pool
     (opsional: thread per proses, karena DNS lookup bersifat I/O-bound).
  4. Mengirim hasil ke Master + heartbeat berisi CPU/Memory (untuk monitoring).

Contoh menjalankan (di tiap node Worker):
    python3 worker.py
    python3 worker.py --master 192.168.1.10 --port 5000
    python3 worker.py --master 192.168.1.10 --processes 4

Opsi simulasi (untuk pengujian di laporan):
    --crash-after 3         -> Worker mati mendadak setelah 3 task selesai (uji Fault Tolerance)
    --simulate-delay 0.01   -> tambahan delay per domain (uji Load Balancing, Worker "lambat")

Dependensi:
    pip install -r requirements.txt  (keduanya opsional tetapi sangat disarankan)
    Tanpa dnspython, resolusi memakai socket.getaddrinfo (tanpa kontrol timeout per query).
"""

import argparse
import json
import multiprocessing as mp
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

try:
    import dns.resolver
    import dns.exception  # noqa: F401
    HAS_DNSPYTHON = True
except ImportError:
    HAS_DNSPYTHON = False

try:
    import psutil
except ImportError:
    psutil = None


# ----------------------------------------------------------------------------
# Bagian inti: resolusi DNS (dipakai juga oleh master.py untuk mode sequential/local)
# ----------------------------------------------------------------------------
def classify_error(exc):
    """Memetakan exception ke status DNS yang mudah dianalisis."""
    name = type(exc).__name__
    if name == "NXDOMAIN":
        return "NXDOMAIN"
    if name in ("Timeout", "LifetimeTimeout"):
        return "TIMEOUT"
    if name == "NoAnswer":
        return "NOANSWER"
    if name == "NoNameservers":
        return "SERVFAIL"
    if isinstance(exc, socket.gaierror):
        if exc.errno == getattr(socket, "EAI_NONAME", -2):
            return "NXDOMAIN"
        if exc.errno == getattr(socket, "EAI_AGAIN", -3):
            return "TIMEOUT"
        return "ERROR"
    if isinstance(exc, socket.timeout):
        return "TIMEOUT"
    return "ERROR"


def make_resolver(cfg):
    resolver = dns.resolver.Resolver(configure=True)
    if cfg.get("nameserver"):
        resolver.nameservers = [cfg["nameserver"]]
    timeout = float(cfg.get("timeout", 3.0))
    resolver.timeout = timeout
    resolver.lifetime = timeout
    return resolver


def resolve_one(domain, resolver, cfg):
    """Resolusi satu domain. Return: [domain, status, 'ip1;ip2', latency_ms]."""
    start = time.perf_counter()
    ips = []
    try:
        if resolver is not None:
            answer = resolver.resolve(domain, cfg.get("record", "A"))
            ips = [r.to_text() for r in answer]
        else:
            info = socket.getaddrinfo(domain, None, socket.AF_INET, socket.SOCK_STREAM)
            ips = sorted({i[4][0] for i in info})
        status = "OK"
    except Exception as exc:  # noqa: BLE001 - kita klasifikasikan semua error DNS
        status = classify_error(exc)
    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    return [domain, status, ";".join(ips), latency_ms]


def resolve_chunk(task_id, domains, cfg, threads=1, simulate_delay=0.0,
                  report_progress=False):
    """
    Satu TASK = satu potongan daftar domain.
    Fungsi ini dijalankan di proses anak (multiprocessing.Pool), jadi harus top-level.
    """
    t0 = time.perf_counter()
    resolver = make_resolver(cfg) if HAS_DNSPYTHON else None
    progress_interval = max(1, len(domains) // 10)
    progress_lock = threading.Lock()
    completed = 0

    def work(domain):
        nonlocal completed
        if simulate_delay:
            time.sleep(simulate_delay)
        result = resolve_one(domain, resolver, cfg)
        if report_progress:
            with progress_lock:
                completed += 1
                count = completed
                if count % progress_interval == 0 or count == len(domains):
                    detail = f"{domain}: {result[1]}"
                    if result[2]:
                        detail += f" ({result[2]})"
                    print(
                        f"[Worker] Task #{task_id} progres "
                        f"{count}/{len(domains)} ({count / len(domains):.0%}) | {detail}",
                        flush=True,
                    )
        return result

    if threads and threads > 1:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            results = list(ex.map(work, domains))
    else:
        results = [work(d) for d in domains]

    return {
        "task_id": task_id,
        "results": results,
        "elapsed": time.perf_counter() - t0,
        "pid": os.getpid(),
    }


# ----------------------------------------------------------------------------
# Worker (komunikasi jaringan)
# ----------------------------------------------------------------------------
class Worker:
    def __init__(self, args):
        self.args = args
        self.processes = args.processes or os.cpu_count() or 1
        self.sock = None
        self.rfile = None
        self.send_lock = threading.Lock()
        self.cnt_lock = threading.Lock()
        self.inflight = 0
        self.completed = 0
        self.stop_event = threading.Event()
        self.cfg = {}
        self.threads = 1
        self.pool = None
        self.worker_id = None

    def discover_master(self):
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            udp.settimeout(1.0)
            request = json.dumps({
                "type": "discover_master",
                "service": "paradns",
            }).encode("utf-8")

            for attempt in range(1, self.args.discover_retries + 1):
                send_errors = []
                for destination in (
                    ("255.255.255.255", self.args.discover_port),
                    ("127.0.0.1", self.args.discover_port),
                ):
                    try:
                        udp.sendto(request, destination)
                    except OSError as exc:
                        send_errors.append(exc)
                if len(send_errors) == 2:
                    print(f"[Worker] Discovery UDP gagal: {send_errors[-1]}")

                deadline = time.monotonic() + 1.0
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    udp.settimeout(remaining)
                    try:
                        data, addr = udp.recvfrom(4096)
                    except socket.timeout:
                        break
                    except OSError as exc:
                        print(f"[Worker] Discovery UDP gagal: {exc}")
                        break

                    try:
                        response = json.loads(data.decode("utf-8"))
                        if response.get("type") != "master_found" or \
                                response.get("service") != "paradns":
                            continue
                        port = int(response.get("port", self.args.port))
                    except (UnicodeDecodeError, ValueError, AttributeError):
                        continue

                    host = addr[0]
                    print(f"[Worker] Master ditemukan otomatis: {host}:{port}")
                    self.args.master = host
                    self.args.port = port
                    return

                print(f"[Worker] Mencari Master otomatis... {attempt}/{self.args.discover_retries}")
                time.sleep(1)
        finally:
            udp.close()
        print("[Worker] Master tidak ditemukan lewat discovery otomatis.")
        sys.exit(1)

    # ---- jaringan -----------------------------------------------------------
    def connect(self):
        for attempt in range(1, self.args.retries + 1):
            try:
                s = socket.create_connection((self.args.master, self.args.port), timeout=10)
                s.settimeout(None)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock = s
                self.rfile = s.makefile("r", encoding="utf-8")
                return
            except OSError as exc:
                print(f"[Worker] Gagal konek ke Master ({exc}). Percobaan {attempt}/{self.args.retries}")
                time.sleep(2)
        print("[Worker] Tidak bisa terhubung ke Master. Keluar.")
        sys.exit(1)

    def send(self, msg):
        data = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")
        with self.send_lock:
            self.sock.sendall(data)

    def safe_send(self, msg):
        try:
            self.send(msg)
            return True
        except OSError:
            self.stop_event.set()
            return False

    def register(self):
        self.send({
            "type": "register",
            "hostname": self.args.name or socket.gethostname(),
            "cpus": os.cpu_count() or 1,
            "processes": self.processes,
        })
        line = self.rfile.readline()
        if not line:
            print("[Worker] Master menutup koneksi saat registrasi.")
            sys.exit(1)
        ack = json.loads(line)
        self.worker_id = ack["worker_id"]
        self.cfg = ack.get("config", {})
        self.threads = self.args.threads or self.cfg.get("threads", 1)
        print(f"[Worker] Terdaftar sebagai worker #{self.worker_id} | proses={self.processes} "
              f"| thread/proses={self.threads} | dnspython={'ya' if HAS_DNSPYTHON else 'tidak'}")

    # ---- pemrosesan task ------------------------------------------------------
    def submit(self, msg):
        tid = msg["task_id"]
        domains = msg["domains"]
        with self.cnt_lock:
            self.inflight += 1
        print(f"[Worker] Menerima task #{tid}: {len(domains)} domain.", flush=True)
        self.pool.apply_async(
            resolve_chunk,
            (tid, domains, self.cfg, self.threads, self.args.simulate_delay, True),
            callback=self.on_done,
            error_callback=lambda exc, t=tid: self.on_error(t, exc),
        )

    def on_done(self, res):
        with self.cnt_lock:
            self.inflight -= 1
            self.completed += 1
            n = self.completed
        status_counts = {}
        for row in res["results"]:
            status_counts[row[1]] = status_counts.get(row[1], 0) + 1
        summary = ", ".join(
            f"{status}={count}" for status, count in sorted(status_counts.items())
        )
        print(
            f"[Worker] Task #{res['task_id']} selesai: {len(res['results'])} domain "
            f"dalam {res['elapsed']:.2f} dtk"
            + (f" | {summary}" if summary else ""),
            flush=True,
        )
        self.safe_send({
            "type": "result",
            "task_id": res["task_id"],
            "results": res["results"],
            "elapsed": res["elapsed"],
            "pid": res["pid"],
        })
        # Simulasi kegagalan Worker (crash mendadak, tanpa pamit ke Master)
        if self.args.crash_after and n >= self.args.crash_after:
            print(f"[SIMULASI] Worker crash setelah {n} task.")
            sys.stdout.flush()
            for p in mp.active_children():
                try:
                    p.kill()
                except Exception:  # noqa: BLE001
                    pass
            os._exit(1)
        self.safe_send({"type": "request"})

    def on_error(self, task_id, exc):
        with self.cnt_lock:
            self.inflight -= 1
        print(f"[Worker] Task {task_id} error: {exc}")
        self.safe_send({"type": "task_failed", "task_id": task_id, "error": str(exc)})
        self.safe_send({"type": "request"})

    def request_later(self):
        if not self.stop_event.is_set():
            self.safe_send({"type": "request"})

    # ---- heartbeat --------------------------------------------------------------
    def heartbeat_loop(self):
        if psutil:
            psutil.cpu_percent(None)  # prime
        while not self.stop_event.wait(self.args.hb_interval):
            with self.cnt_lock:
                inflight = self.inflight
            msg = {
                "type": "heartbeat",
                "inflight": inflight,
                "cpu": psutil.cpu_percent(None) if psutil else None,
                "mem": psutil.virtual_memory().percent if psutil else None,
            }
            if not self.safe_send(msg):
                break

    # ---- main loop ----------------------------------------------------------------
    def run(self):
        if not self.args.master:
            self.discover_master()
        # Pool dibuat sebelum socket TCP dibuka agar proses anak tidak mewarisi socket.
        self.pool = mp.Pool(self.processes)
        self.connect()
        self.register()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        for _ in range(self.processes):  # satu request per slot proses
            self.send({"type": "request"})

        try:
            for line in self.rfile:
                msg = json.loads(line)
                mtype = msg.get("type")
                if mtype == "task":
                    self.submit(msg)
                elif mtype == "wait":
                    threading.Timer(self.args.poll_interval, self.request_later).start()
                elif mtype == "shutdown":
                    print("[Worker] Master meminta shutdown (semua task selesai).")
                    break
        except (OSError, ValueError) as exc:
            print(f"[Worker] Koneksi ke Master terputus: {exc}")
        finally:
            self.stop_event.set()
            try:
                self.pool.terminate()
            except Exception:  # noqa: BLE001
                pass
            try:
                self.sock.close()
            except OSError:
                pass
        print(f"[Worker] Selesai. Total task diproses: {self.completed}")


def parse_args():
    p = argparse.ArgumentParser(description="Worker - Distributed DNS Lookup")
    p.add_argument("--master", default="", help="IP/hostname Master (kosong = cari Master otomatis)")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--discover-port", type=int, default=5001,
                   help="Port UDP untuk mencari Master otomatis")
    p.add_argument("--discover-retries", type=int, default=10,
                   help="Jumlah percobaan pencarian Master otomatis")
    p.add_argument("--processes", type=int, default=0, help="Jumlah proses (default: semua core)")
    p.add_argument("--threads", type=int, default=0,
                   help="Thread per proses (default: ikut konfigurasi Master)")
    p.add_argument("--name", default="", help="Nama Worker (default: hostname)")
    p.add_argument("--hb-interval", type=float, default=2.0, help="Interval heartbeat (detik)")
    p.add_argument("--poll-interval", type=float, default=0.5, help="Jeda request ulang saat diminta menunggu")
    p.add_argument("--retries", type=int, default=10, help="Percobaan koneksi awal ke Master")
    p.add_argument("--crash-after", type=int, default=0,
                   help="[SIMULASI] crash setelah N task selesai (0 = nonaktif)")
    p.add_argument("--simulate-delay", type=float, default=0.0,
                   help="[SIMULASI] delay tambahan per domain (detik) untuk meniru Worker lambat")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        Worker(args).run()
    except KeyboardInterrupt:
        print("\n[Worker] Dihentikan pengguna.")


if __name__ == "__main__":
    mp.freeze_support()
    main()
